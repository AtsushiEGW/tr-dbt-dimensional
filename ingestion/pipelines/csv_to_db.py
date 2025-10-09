# ingestion/pipelines/csv_to_db.py
from __future__ import annotations

import argparse
import io
import os
import sys
import shutil
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
        merged_tables[name] = (defaults | spec)  # Py3.9+ dict merge
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


def _ensure_utf8_copy(path: Path, src_encoding: str) -> Path:
    """
    入力が UTF-8 以外なら UTF-8 に変換した一時ファイルを作って返す。
    UTF-8/UTF-8-SIG の場合は元の path を返す。
    """
    enc = (src_encoding or "utf-8").lower()
    if enc in ("utf-8", "utf8", "utf-8-sig"):
        return path
    dst = path.with_suffix(path.suffix + ".utf8.tmp")
    with path.open("r", encoding=src_encoding, newline="") as src, dst.open("w", encoding="utf-8", newline="") as out:
        for line in src:
            out.write(line)
    return dst


def _read_header_raw(path: Path, encoding: str, skiprows: int = 0) -> list[str]:
    """
    先頭の“ヘッダ行”を取得。skiprows > 0 なら読み飛ばしてから CSV 1行読取。
    重複カラム名をそのまま得るため pandas ではなく csv.reader を使う。
    """
    import csv
    with path.open("r", encoding=encoding, newline="") as f:
        for _ in range(skiprows):
            if f.readline() == "":
                return []
        reader = csv.reader(f)
        try:
            hdr = next(reader)
        except StopIteration:
            return []
    # BOM の除去（UTF-8-SIG）
    if hdr and isinstance(hdr[0], str) and hdr[0].startswith("\ufeff"):
        hdr[0] = hdr[0].lstrip("\ufeff")
    return hdr


def _analyze_headers(files: list[Path], encoding: str, skiprows: int) -> tuple[list[str], dict[Path, list[str]]]:
    """
    全ファイルのヘッダを解析し、
      - union_cols: 和集合（各ベース名の最大多重度分だけ xxx_1..xxx_k を展開。最初の出現順）
      - norm_by_file: 各ファイルに対する“正規化済みヘッダ名配列”
    """
    if not files:
        return [], {}

    order: list[str] = []
    seen: set[str] = set()
    max_mult: dict[str, int] = {}
    per_file_raw: dict[Path, list[str]] = {}

    for f in files:
        hdr = _read_header_raw(f, encoding=encoding, skiprows=skiprows)
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


def _dedupe_temp_by_pk(engine: Engine, temp_fqtn: str, pk_cols: list[str]):
    """
    TEMP 内の主キー重複を「後勝ち（最後に入った行が残る）」に正規化。
    これで ON CONFLICT が 1回の INSERT に対して複数ヒットせず安全に動く。
    """
    if not pk_cols:
        return
    pk_eq = " AND ".join([f't."{c}" = d."{c}"' for c in pk_cols])
    sql = f"""
        DELETE FROM {temp_fqtn} t
        USING {temp_fqtn} d
        WHERE {pk_eq}
          AND t.ctid < d.ctid;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


# ---------------------------
# COPY
# ---------------------------
def _copy_df_to_table(engine: Engine, df: pd.DataFrame, table_fqtn: str, columns: list[str]):
    """DataFrame を CSV にして COPY。空欄は NULL '' で本物 NULL に変換。"""
    if df.empty:
        return
    df2 = df.reindex(columns=columns)
    df2 = df2.where(pd.notna(df2), "")  # NaN/NA → ""

    buf = io.StringIO()
    df2.to_csv(buf, index=False, header=False)
    buf.seek(0)

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        try:
            cols_sql = ", ".join([f'"{c}"' for c in columns])
            cur.copy_expert(
                sql=f'COPY {table_fqtn} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, NULL \'\')',
                file=buf,
            )
        finally:
            cur.close()
        raw_conn.commit()
    finally:
        raw_conn.close()


# ---------------------------
# TEMP TABLE
# ---------------------------
def _make_temp_text_table(engine: Engine, columns: list[str]) -> str:
    """TEXT 列の TEMP TABLE を作成して FQTN (pg_temp.<name>) を返す。"""
    temp_name = f"stg_{int(datetime.now().timestamp())}_{os.getpid()}"
    temp_fqtn = f"pg_temp.{temp_name}"
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TEMP TABLE {temp_fqtn} ({cols_sql});'))
    return temp_fqtn


# ---------------------------
# UPSERT
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
    テーブル設定（encoding/skiprows など）を反映しつつ、
    db_ingestion 配下の CSV → UTF-8 正規化 → TEMP → UPSERT。
    """
    folder = cfg["folder"]
    pattern = cfg.get("filename_glob", "*.csv")
    pk_cols = cfg["primary_key"]
    encoding = cfg.get("encoding", "utf-8")    # tables.yml の defaults からマージ済み
    skiprows = int(cfg.get("skiprows", 0))     # 同上

    schema = TARGET_SCHEMA
    target_base = cfg.get("target_table", table_name)
    target_fqtn = f'"{schema}"."{target_base}"'

    # 1) 元 CSV の列挙
    src_files = _iter_csv_files(csv_root, folder, pattern)
    if not src_files:
        print(f"[{table_name}] No CSV files under {(csv_root / folder)}")
        return

    # 2) 必要なら UTF-8 一時ファイルへ変換（cp932 等）
    processed_files: list[Path] = []
    tmp_to_cleanup: list[Path] = []
    for f in src_files:
        g = _ensure_utf8_copy(f, encoding)
        processed_files.append(g)
        if g != f:
            tmp_to_cleanup.append(g)

    try:
        # 3) ヘッダ解析（skiprows を反映）
        union_cols, norm_by_file = _analyze_headers(processed_files, encoding="utf-8", skiprows=skiprows)
        if not union_cols:
            print(f"[{table_name}] WARNING: header not found")
            return

        # 4) 取り込み先テーブルの用意
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

        # 5) TEMP(TEXT) 作成
        temp_fqtn = _make_temp_text_table(engine, db_columns)
        if pk_cols:
            with engine.begin() as conn:
                idx_cols = ", ".join([f'"{c}"' for c in pk_cols])
                conn.execute(text(f'CREATE INDEX ON {temp_fqtn} ({idx_cols});'))

        # 6) 読み込み & COPY（UTF-8 ファイル + skiprows）
        for f in processed_files:
            print(f"[{table_name}] Loading {f}")
            norm_cols = norm_by_file[f]

            # ここがポイント:
            #  - header=0 で“CSVの本来のヘッダ”をヘッダとして解釈し、データ化しない
            #  - engine="python" で末尾カンマ等の可変列に強くする
            #  - usecols=range(len(norm_cols)) で、末尾カンマで増える空列を物理位置で無視
            #  - 読み込み後に列名を正規化ヘッダに差し替え（重複ヘッダの一意化を維持）
            for chunk in pd.read_csv(
                f,
                header=0,                       # ← ヘッダ行をデータに入れない
                dtype=str,
                chunksize=chunksize,
                na_filter=True,
                keep_default_na=False,          # 'NA' などは文字として扱う
                na_values=[""],                 # 空欄のみ欠損 → COPY で NULL
                encoding="utf-8",
                skiprows=skiprows,              # 先頭のメタ行などをスキップ
                engine="python",                # 可変列/末尾カンマ対策
                usecols=range(len(norm_cols)),  # 余分な空列（末尾カンマ起因）を無視
            ):
                # 列名を“正規化済みヘッダ”に揃える
                chunk.columns = norm_cols

                # PK 欠損を捨てる（UPSERT できない）
                for pk in pk_cols:
                    if pk in chunk.columns:
                        chunk = chunk[chunk[pk].notna() & (chunk[pk] != "")]

                # DB 列に合わせる（欠け列を追加、余分は落とす）
                for c in db_columns:
                    if c not in chunk.columns:
                        chunk[c] = pd.NA
                chunk = chunk[db_columns]

                _copy_df_to_table(engine, chunk, temp_fqtn, db_columns)

        # 7) TEMP 内で主キー重複を「後勝ち」に正規化
        _dedupe_temp_by_pk(engine, temp_fqtn, pk_cols)

        # 8) UPSERT
        non_key_cols = [c for c in db_columns if c not in pk_cols]
        set_clause = (
            "SET " + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in non_key_cols])
            if non_key_cols else "DO NOTHING"
        )
        upsert_sql = f"""
            INSERT INTO {target_fqtn} ({", ".join([f'"{c}"' for c in db_columns])})
            SELECT {", ".join([f'"{c}"' for c in db_columns])} FROM {temp_fqtn}
            ON CONFLICT ({", ".join([f'"{c}"' for c in pk_cols])})
            DO UPDATE {set_clause};
        """
        with engine.begin() as conn:
            conn.execute(text(upsert_sql))

        print(f"[{table_name}] Upsert completed.")

    finally:
        # 変換で作った一時 UTF-8 ファイルを掃除
        for p in tmp_to_cleanup:
            try:
                p.unlink()
            except Exception:
                pass


# ---------------------------
# SNAPSHOT (Parquet)
# ---------------------------
def snapshot_table_to_parquet(engine: Engine, table_name: str, out_root: Path):
    schema = TARGET_SCHEMA
    fqtn = f'"{schema}"."{table_name}"'
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