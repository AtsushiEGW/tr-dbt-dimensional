import csv
import argparse
import io
import shutil
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import pandas as pd
import os
from pathlib import Path
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict, List, Tuple
from ingestion.db_ingestion.utils import (
    get_engine, ensure_schema, get_paths, get_retention_days, today_stamp,
    table_exists, get_table_columns, create_text_table, add_missing_text_columns
)




# -------------------------------------------------
# Paths / Config
# -------------------------------------------------
HERE: Path = Path(__file__).parent
CFG_PATH: Path = HERE / "config" / "tables.yml"
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "raw")
PATHS = get_paths() # dict[str, Path] を想定


# -------------------------------------------------
# ヘルパー：設定 & ファイル列挙
# -------------------------------------------------
def load_config() -> dict:
    with CFG_PATH.open("r", encoding="utf8") as f:
        cfg = yaml.safe_load(f)

        defaults = cfg.get("defaults", {})
        tables = cfg.get("tables", {})

        for table_name, spec in tables.items():
            # ネストした設定はspecですべて上書きされてしまうため注意
            merged = defaults | spec
            tables[table_name] = merged

        cfg["tables"] = tables
        return cfg


def _iter_csv_files(folder_root: Path, folder: str, pattern: str) -> list[Path]:
    """folder_root/folder 配下で pattern (ex. "links_*.csv") にマッチする CSV をファイル名の昇順で返す"""
    base = folder_root / folder
    if not base.exists():
        print(f"[warn] base folder not found: {base}")
    files = sorted(base.rglob(pattern), key=lambda p: str(p))
    return files

# -------------------------------------------------
# ヘッダ正規化（重複列対応）
# -------------------------------------------------
def _read_raw_header(path: Path, encoding: str | None = None) -> list[str]:
    """CSVのheaderだけを標準csvで読む。高速&pandas非依存"""
    with path.open("r", encoding=encoding or "utf-8", newline="") as f:
        reader = csv.reader(f)
        return next(reader, [])


def _plan_normalized_headers(files: list[Path]) -> tuple[list[str], dict[Path, list[str]], dict[str, int]] :
    """
    すべてのCSVのヘッダを見て、同名列の"最大多重度"を求め、
    各ファイルのヘッダを name_1, name_2, ... に正規化する計画を作る
    
    return:
        union_cols: 全ファイルでの正規化後カラムの和集合(順序は最初のファイル優先、以後は末尾追加)
        per_file_cols : 各ファイルの正規化後カラム名リスト
        max_dup_count: {ベース名: 最大多重度}
    """
    if not files:
        return [], {}, {}

    # 1) 各ファイルへのraw header
    raw_by_file: dict[Path, list[str]] = {f: _read_raw_header(f) for f in files}

    # 2) 列ごとの最大多重度
    max_dup_count: dict[str, int] = defaultdict(int)
    for cols in raw_by_file.values():
        cnt = Counter(cols)
        for name, n in cnt.items():
            if n > max_dup_count[name]:
                max_dup_count[name] = n

    # 3) 正規化（シリーズ化）
    def normalize(cols: list[str]) -> list[str]:
        seen_idx: dict[str, int] = defaultdict(int)
        out: list[str] = []
        for name in cols:
            seen_idx[name] += 1
            k = seen_idx[name]
            if max_dup_count[name] > 1:
                out.append(f"{name}_{k}")  # 多重度>1なら系列化
            else:
                out.append(name)          # 単独はそのまま
        return out

    per_file_cols: dict[Path, list[str]] = {f: normalize(raw_by_file[f]) for f in files}

    # 4) 正規化後の和集合（順序は最初を基準に、新規は末尾）
    first = per_file_cols[files[0]]
    union_cols: list[str] = first[:]
    seen = set(union_cols)
    for f in files[1:]:
        for c in per_file_cols[f]:
            if c not in seen:
                seen.add(c)
                union_cols.append(c)

    return union_cols, per_file_cols, dict(max_dup_count)

# 互換のためのラッパ（既存名）
def _union_csv_headers(files: list[Path]) -> tuple[list[str], dict[Path, list[str]], dict[str, int]]:
    return _plan_normalized_headers(files)

# -------------------------------------------------
# 一時テーブル & COPY
# -------------------------------------------------
def _make_temp_text_table(engine: Engine, columns: list[str]) -> str:
    temp_name = f"stg_{int(datetime.now().timestamp())}_{os.getpid()}"
    temp_fqtn = f"pg_temp.{temp_name}"
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TEMP TABLE {temp_fqtn} ({cols_sql});'))
    return temp_fqtn

def _copy_df_to_table(engine: Engine, df: pd.DataFrame, table_fqtn: str, columns: list[str]):
    """
    DataFrame -> COPY FROM STDIN (CSV)。
    欠損は空文字にしてCSV化し、COPY側の NULL '' により本物のNULLへ。
    """
    if df.empty:
        return

    df2 = df.reindex(columns=columns)
    df2 = df2.where(pd.notna(df2), "")  # NaN -> ""

    buf = io.StringIO()
    df2.to_csv(buf, index=False, header=False)
    buf.seek(0)

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.copy_expert(
                sql=(
                    f'COPY {table_fqtn} ({", ".join([f"""\"{c}\"""" for c in columns])}) '
                    f"FROM STDIN WITH (FORMAT CSV, NULL '')"
                ),
                file=buf,
            )
        raw_conn.commit()
    finally:
        raw_conn.close()

# -------------------------------------------------
# コア：UPSERT
# -------------------------------------------------
def upsert_table(
    engine: Engine,
    table_name: str,
    cfg: dict,
    csv_root: Path,
    chunksize: int = 200_000,
    auto_add_columns: bool = False,
):
    """
    CSV群を読み→正規化（重複列は _1.._k）→ TEMP(TEXT) → UPSERT。
    - rawはTEXTのみ
    - 複合PKOK
    - 空欄は本物のNULL
    - 列の和集合は「最初優先・新規は末尾」
    """
    folder = cfg["folder"]
    pattern = cfg.get("filename_glob", "*.csv")
    pk_cols = cfg["primary_key"]
    target_base = cfg.get("target_table", table_name)
    schema = TARGET_SCHEMA
    target_fqtn = f'"{schema}"."{target_base}"'

    files = _iter_csv_files(csv_root, folder, pattern)
    if not files:
        print(f"[{table_name}] No CSV files under {csv_root / folder}")
        return

    # --- 正規化計画（ここが重複列対応のキモ） ---
    csv_columns, norm_by_file, max_dup = _union_csv_headers(files)

    # ログ（Tip①）
    series_map = {name: [f"{name}_{i}" for i in range(1, n+1)] for name, n in max_dup.items() if n > 1}
    if series_map:
        print(f"[{table_name}] normalized series: {series_map}")
    for f in files:
        print(f"[{table_name}] header ({f.name}) -> {norm_by_file[f]}")
    print(f"[{table_name}] union columns       -> {csv_columns}")

    # --- ターゲットテーブル準備 ---
    if not table_exists(engine, schema, target_base):
        create_text_table(engine, schema, target_base, csv_columns, pk_cols)
        db_columns = csv_columns[:]
    else:
        db_columns = get_table_columns(engine, schema, target_base)
        missing = [c for c in csv_columns if c not in db_columns]
        if missing:
            if auto_add_columns:
                add_missing_text_columns(engine, schema, target_base, missing)
                db_columns = get_table_columns(engine, schema, target_base)
                print(f'[{table_name}] Added new columns: {", ".join(missing)}')
            else:
                print(f'[{table_name}] WARNING: Ignoring new CSV columns (use --auto-add-columns to add): {", ".join(missing)}')

    # --- TEMP TABLE 作成 ---
    temp_fqtn = _make_temp_text_table(engine, db_columns)

    # --- 読み込み & COPY（names=正規化済み、pandasの自動改名は無効化） ---
    for f in files:
        print(f"[{table_name}] Loading {f}")
        norm_cols = norm_by_file[f]

        for chunk in pd.read_csv(
            str(f),
            header=0,                # 先頭行は元ヘッダ（読み飛ばされる）
            names=norm_cols,         # 正規化後のヘッダに置き換え
            mangle_dupe_cols=False,  # pandasの .1 付与を抑止（既に一意）
            dtype=str,
            chunksize=chunksize,
            na_filter=True,
            keep_default_na=False,
            na_values=[""],          # 空欄のみ欠損 → COPYでNULL
        ):
            # DB列に合わせて整形：欠け列は欠損、余分は落とす
            for c in db_columns:
                if c not in chunk.columns:
                    chunk[c] = pd.NA
            chunk = chunk[db_columns]
            _copy_df_to_table(engine, chunk, temp_fqtn, db_columns)

    # --- UPSERT ---
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

# -------------------------------------------------
# スナップショット（Parquet）
# -------------------------------------------------
def snapshot_table_to_parquet(engine: Engine, table_name: str, out_root: Path):
    schema = TARGET_SCHEMA
    fqtn = f'"{schema}"."{table_name}"'
    date_folder = out_root / datetime.now().strftime("%Y%m%d")
    date_folder.mkdir(parents=True, exist_ok=True)
    out_path = date_folder / f"{table_name}.parquet"

    print(f"[snapshot] {fqtn} -> {out_path}")
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {fqtn}", conn)
    df.to_parquet(str(out_path), index=False)
    print(f"[snapshot] Wrote {out_path}")

# -------------------------------------------------
# CSVクリーンアップ
# -------------------------------------------------
def clean_old_csvs(csv_root: Path, archive_root: Path | None, retention_days: int, dry_run: bool = True):
    cutoff = datetime.now() - timedelta(days=retention_days)
    total = 0

    for table_folder in csv_root.iterdir():
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
                        rel = f.relative_to(csv_root)
                        dest = archive_root / rel
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(f), str(dest))
                        print(f"[moved] {f} -> {dest}")
                    else:
                        f.unlink()
                        print(f"[removed] {f}")

    print(f"[clean] targets={total}, dry_run={dry_run}")

# -------------------------------------------------
# CLI
# -------------------------------------------------
def cmd_ingest(args):
    engine = get_engine()
    ensure_schema(engine, TARGET_SCHEMA)
    tables = load_config()

    targets = [args.table] if args.table else list(tables.keys())
    for name in targets:
        if name not in tables:
            print(f"Unknown table '{name}'. Available: {', '.join(tables.keys())}")
            sys.exit(1)
        upsert_table(
            engine,
            name,
            tables[name],
            PATHS["CSV_ROOT"],
            chunksize=args.chunksize,
            auto_add_columns=args.auto_add_columns,
        )

def cmd_snapshot(args):
    engine = get_engine()
    tables = load_config()
    targets = [args.table] if args.table else list(tables.keys())
    for name in targets:
        cfg = tables[name]
        snapshot_table_to_parquet(engine, cfg.get("target_table", name), PATHS["PARQUET_ROOT"])

def cmd_clean(args):
    days = get_retention_days()
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
    p_ing.add_argument("--chunksize", type=int, default=200_000, help="pandas read_csv chunksize")
    p_ing.add_argument("--auto-add-columns", action="store_true",
                       help="if CSV has new columns, ALTER TABLE ADD COLUMN (TEXT)")
    p_ing.set_defaults(func=cmd_ingest)

    p_snap = sub.add_parser("snapshot", help="export tables from Postgres to Parquet")
    p_snap.add_argument("--table", help="single table to snapshot (default: all)")
    p_snap.set_defaults(func=cmd_snapshot)

    p_clean = sub.add_parser("clean", help="delete or archive old CSV files")
    p_clean.add_argument("--archive", action="store_true", help="move files to archive instead of deleting")
    p_clean.add_argument("--dry-run", action="store_true", help="only print targets")
    p_clean.set_defaults(func=cmd_clean)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()