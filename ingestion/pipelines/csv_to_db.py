from __future__ import annotations

import argparse
import io
import os
import sys
import shutil
import time
import secrets
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.utils import (
    get_engine, ensure_schema, get_paths,
    table_exists, get_table_columns, create_text_table, add_missing_text_columns
)

# ---------------------------
# Paths / Config
# ---------------------------
HERE: Path = Path(__file__).parent
CFG_PATH: Path = HERE.parent / "config" / "tables.yml"   # ingestion/config/tables.yml
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "raw")
PATHS = get_paths()  # dict[str, Path]


# ---------------------------
# Config loader (defaults merge)
# ---------------------------
def load_config() -> dict:
    with CFG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if "tables" not in cfg:
        raise ValueError("config must contain 'tables'")
    defaults = cfg.get("defaults", {})
    merged_tables = {}
    for name, spec in cfg["tables"].items():
        merged_tables[name] = (defaults | spec)  # py3.9+: dict merge
    cfg["tables"] = merged_tables
    return cfg


# ---------------------------
# Helpers: files & headers
# ---------------------------
def _iter_csv_files(folder_root: Path, folder: str, pattern: str) -> list[Path]:
    """folder_root/folder 配下で pattern にマッチする CSV を昇順列挙"""
    base = folder_root / folder
    if not base.exists():
        print(f"[warn] base folder not found: {base}")
        return []
    return sorted(base.glob(pattern), key=lambda p: str(p))


def _read_header_raw(path: Path, encoding: str) -> list[str]:
    """pandas に任せず、1行目を生で読む（重複ヘッダをそのまま取得）"""
    import csv
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        try:
            hdr = next(reader)
        except StopIteration:
            return []
    # BOM (UTF-8 SIG) の除去
    if hdr and isinstance(hdr[0], str) and hdr[0].startswith("\ufeff"):
        hdr[0] = hdr[0].lstrip("\ufeff")
    return hdr


def _analyze_headers(files: list[Path], encoding: str) -> tuple[list[str], dict[Path, list[str]]]:
    """
    全ファイルのヘッダを解析し、
      - union_cols: 和集合（各ベース名の最大多重度だけ xxx_1..xxx_k を並べる / 最初の出現順）
      - norm_by_file: 各ファイルでの “正規化済みヘッダ名” を返す
    """
    if not files:
        return [], {}
    order: list[str] = []
    seen: set[str] = set()
    max_mult: dict[str, int] = {}
    per_file_raw: dict[Path, list[str]] = {}

    for f in files:
        hdr = _read_header_raw(f, encoding=encoding)
        per_file_raw[f] = hdr
        counts: dict[str, int] = {}
        for col in hdr:
            counts[col] = counts.get(col, 0) + 1
            if col not in seen:
                seen.add(col)
                order.append(col)
        for col, c in counts.items():
            if c > max_mult.get(col, 0):
                max_mult[col] = c

    union_cols: list[str] = []
    for base in order:
        k = max_mult[base]
        if k == 1:
            union_cols.append(base)
        else:
            union_cols.extend([f"{base}_{i}" for i in range(1, k + 1)])

    norm_by_file: dict[Path, list[str]] = {}
    for f, hdr in per_file_raw.items():
        counters: dict[str, int] = {}
        norm: list[str] = []
        for col in hdr:
            counters[col] = counters.get(col, 0) + 1
            cnt = counters[col]
            if max_mult[col] == 1:
                norm.append(col)
            else:
                norm.append(f"{col}_{cnt}")
        norm_by_file[f] = norm

    return union_cols, norm_by_file


# ---------------------------
# Helpers: TEMP & COPY
# ---------------------------
def _unique_temp_name(table_name: str) -> str:
    """ミリ秒＋乱数でユニークな一時テーブル名を生成"""
    ms = int(time.time() * 1000)
    rnd = secrets.token_hex(3)
    safe = "".join(ch if ch.isalnum() else "_" for ch in table_name)
    return f"tmp_{safe}_{ms}_{rnd}"


def _copy_df_via_copy(dbapi_conn, df: pd.DataFrame, temp_name: str, columns: list[str]):
    """DBAPI接続で COPY ... FROM STDIN を実行（NaN→空文字→NULL '' で本物のNULL化）"""
    if df.empty:
        return
    # 列順合わせ + NaN を空文字へ
    df2 = df.reindex(columns=columns).where(pd.notna(df), "")
    buf = io.StringIO()
    df2.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cur = dbapi_conn.cursor()
    try:
        cols_sql = ", ".join([f'"{c}"' for c in columns])
        cur.copy_expert(
            sql=f'COPY "{temp_name}" ({cols_sql}) FROM STDIN WITH (FORMAT CSV, NULL \'\')',
            file=buf,
        )
    finally:
        cur.close()


# ---------------------------
# UPSERT（単一接続で完結）
# ---------------------------
def upsert_table(
    engine: Engine,
    table_name: str,
    cfg: dict,
    csv_root: Path,
    chunksize: int,
    auto_add_columns: bool,
):
    """
    指定テーブルの CSV を列挙 → TEMP(TEXT) に積む → TEMP 内でPK重複を後勝ちで正規化 → ON CONFLICT でUPSERT。
    すべて同一コネクションの中で完結させる。
    """
    folder = cfg["folder"]
    pattern = cfg.get("filename_glob", "*.csv")
    pk_cols = cfg["primary_key"]
    encoding = cfg.get("encoding", "utf-8")

    schema = TARGET_SCHEMA
    target_base = cfg.get("target_table", table_name)
    target_fqtn = f'"{schema}"."{target_base}"'

    # 1) 対象CSV
    files = _iter_csv_files(csv_root, folder, pattern)
    if not files:
        print(f"[{table_name}] No CSV files under {(csv_root / folder)}")
        return

    # 2) ヘッダ解析
    union_cols, norm_by_file = _analyze_headers(files, encoding=encoding)
    if not union_cols:
        print(f"[{table_name}] WARNING: header not found")
        return

    # 3) 取り込み先テーブル
    if not table_exists(engine, schema, target_base):
        create_text_table(engine, schema, target_base, union_cols, pk_cols)
        db_columns = union_cols[:]
    else:
        db_columns = get_table_columns(engine, schema, target_base)
        missing = [c for c in union_cols if c not in db_columns]
        if missing:
            if auto_add_columns:
                add_missing_text_columns(engine, schema, target_base, missing)
                db_columns = get_table_columns(engine, schema, target_base)
                print(f'[{table_name}] Added new columns: {", ".join(missing)}')
            else:
                print(f'[{table_name}] WARNING: New columns ignored (use --auto-add-columns): {", ".join(missing)}')

    # 4) 単一接続で TEMP→COPY→重複除去→UPSERT を完結
    with engine.begin() as conn:
        # TEMP 作成（ON COMMIT DROP で自動掃除）
        temp_name = _unique_temp_name(target_base)
        cols_sql = ", ".join([f'"{c}" TEXT' for c in db_columns])
        conn.execute(text(f'CREATE TEMP TABLE "{temp_name}" ({cols_sql}) ON COMMIT DROP;'))

        # 任意：PK インデックス
        if pk_cols:
            idx_cols = ", ".join([f'"{c}"' for c in pk_cols])
            conn.execute(text(f'CREATE INDEX ON "{temp_name}" ({idx_cols});'))

        # 同一セッションの DBAPI 接続（COPYで使用）
        # SQLAlchemy 2.x の場合:
        dbapi_conn = getattr(conn.connection, "driver_connection", None)
        if dbapi_conn is None:  # バージョン差異ケア
            dbapi_conn = getattr(conn.connection, "connection")

        # 5) CSV 読み込み → TEMP へ COPY
        for f in files:
            print(f"[{table_name}] Loading {f}")
            norm_cols = norm_by_file[f]
            for chunk in pd.read_csv(
                f,
                header=0,
                names=norm_cols,
                dtype=str,
                chunksize=chunksize,
                na_filter=True,
                keep_default_na=False,
                na_values=[""],         # 空欄のみ欠損 → COPY の NULL '' で本物の NULL
                encoding=encoding,
            ):
                # PK 欠損は弾く（ON CONFLICT のキーにできないため）
                for pk in pk_cols:
                    if pk in chunk.columns:
                        chunk = chunk[chunk[pk].notna() & (chunk[pk] != "")]

                # DB列に合わせ（欠け列は欠損で追加、余分は落とす）
                for c in db_columns:
                    if c not in chunk.columns:
                        chunk[c] = pd.NA
                chunk = chunk[db_columns]

                _copy_df_via_copy(dbapi_conn, chunk, temp_name, db_columns)

        # 6) TEMP 内の主キー重複を “後勝ち” で 1 行に
        if pk_cols:
            pk_eq = " AND ".join([f't."{c}" = d."{c}"' for c in pk_cols])
            conn.execute(text(f'''
                DELETE FROM "{temp_name}" t
                USING "{temp_name}" d
                WHERE {pk_eq}
                  AND t.ctid < d.ctid;
            '''))

        # 7) UPSERT
        non_key_cols = [c for c in db_columns if c not in pk_cols]
        set_clause = (
            "SET " + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in non_key_cols])
            if non_key_cols else "DO NOTHING"
        )
        cols_list = ", ".join([f'"{c}"' for c in db_columns])
        pk_list = ", ".join([f'"{c}"' for c in pk_cols])

        conn.execute(text(f'''
            INSERT INTO {target_fqtn} ({cols_list})
            SELECT {cols_list} FROM "{temp_name}"
            ON CONFLICT ({pk_list}) DO UPDATE {set_clause};
        '''))

    print(f"[{table_name}] Upsert completed.")


# ---------------------------
# SNAPSHOT (Parquet) - 存在チェック付き
# ---------------------------
def snapshot_table_to_parquet(engine: Engine, table_name: str, out_root: Path):
    schema = TARGET_SCHEMA
    fqtn = f'"{schema}"."{table_name}"'

    exists_sql = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.connect() as c:
        if c.execute(text(exists_sql), {"schema": schema, "table": table_name}).scalar() is None:
            print(f'[snapshot][skip] table not found: {fqtn}')
            return

    date_folder = out_root / datetime.now().strftime("%Y%m%d")
    date_folder.mkdir(parents=True, exist_ok=True)
    out_path = date_folder / f"{table_name}.parquet"

    print(f"[snapshot] {fqtn} -> {out_path}")
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {fqtn}", conn)
    df.to_parquet(out_path, index=False)
    print(f"[snapshot] Wrote {out_path}")


# ---------------------------
# CLEAN CSVs (db_ingestion 配下)
# ---------------------------
def clean_old_csvs(csv_root: Path, archive_root: Path | None, retention_days: int, dry_run: bool = True):
    cutoff = datetime.now() - timedelta(days=retention_days)
    total = 0
    for table_folder in csv_root.rglob("*"):
        if not table_folder.is_dir():
            continue
        for f in table_folder.glob("*.csv"):
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                total += 1
                if dry_run:
                    print(f"[dry-run] remove {f}")
                else:
                    if archive_root:
                        dest = archive_root / f.relative_to(csv_root)
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(f), str(dest))
                        print(f"[moved] {f} -> {dest}")
                    else:
                        f.unlink()
                        print(f"[removed] {f}")
    print(f"[clean] targets={total}, dry_run={dry_run}")


# ---------------------------
# CLI
# ---------------------------
def cmd_ingest(args):
    engine = get_engine()
    ensure_schema(engine, TARGET_SCHEMA)
    cfg = load_config()
    tables = cfg["tables"]

    targets = [args.table] if args.table else list(tables.keys())
    for name in targets:
        if name not in tables:
            print(f"Unknown table '{name}'. Available: {', '.join(tables.keys())}")
            sys.exit(1)
        spec = tables[name]
        upsert_table(
            engine=engine,
            table_name=name,
            cfg=spec,
            csv_root=PATHS["CSV_ROOT"],
            chunksize=spec.get("chunksize", args.chunksize),
            auto_add_columns=args.auto_add_columns,
        )


def cmd_snapshot(args):
    engine = get_engine()
    cfg = load_config()
    targets = [args.table] if args.table else list(cfg["tables"].keys())
    for name in targets:
        spec = cfg["tables"][name]
        snapshot_table_to_parquet(engine, spec.get("target_table", name), PATHS["PARQUET_ROOT"])


def cmd_clean(args):
    days = int(os.getenv("RETENTION_DAYS", "60"))
    archive = PATHS["ARCHIVE_ROOT"] if args.archive else None
    clean_old_csvs(PATHS["CSV_ROOT"], archive, days, dry_run=args.dry_run)


def main():
    parser = argparse.ArgumentParser(
        prog="csv_to_db",
        description="CSV ingestion -> Postgres upsert (TEXT + NULL blanks) -> Parquet snapshot",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="ingest CSVs and UPSERT into Postgres (raw TEXT)")
    p_ing.add_argument("--table", help="single table to ingest (default: all)")
    p_ing.add_argument("--chunksize", type=int, default=200_000, help="pandas read_csv chunksize (fallback)")
    p_ing.add_argument("--auto-add-columns", action="store_true",
                       help="if CSV has new columns, ALTER TABLE ADD COLUMN (TEXT)")
    p_ing.set_defaults(func=cmd_ingest)

    p_snap = sub.add_parser("snapshot", help="export tables from Postgres to Parquet")
    p_snap.add_argument("--table", help="single table to snapshot (default: all)")
    p_snap.set_defaults(func=cmd_snapshot)

    p_clean = sub.add_parser("clean", help="delete or archive old CSV files under db_ingestion")
    p_clean.add_argument("--archive", action="store_true", help="move files to archive instead of deleting")
    p_clean.add_argument("--dry-run", action="store_true", help="only print targets")
    p_clean.set_defaults(func=cmd_clean)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()