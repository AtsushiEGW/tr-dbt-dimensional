# ingestion/pipelines/csv_to_db.py
from __future__ import annotations

import argparse
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
CFG_PATH: Path = HERE.parent / "config" / "tables.yml"
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "raw")
PATHS = get_paths()


def load_config() -> dict:
    with CFG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if "tables" not in cfg:
        raise ValueError("config must contain 'tables'")
    defaults = cfg.get("defaults", {})
    merged_tables = {}
    for name, spec in cfg["tables"].items():
        merged_tables[name] = (defaults | spec)
    cfg["tables"] = merged_tables
    return cfg


def _iter_csv_files(folder_root: Path, folder: str, pattern: str) -> list[Path]:
    base = folder_root / folder
    if not base.exists():
        print(f"[warn] base folder not found: {base}")
        return []
    return sorted(base.glob(pattern), key=lambda p: str(p))


def _ensure_utf8_copy(path: Path, src_encoding: str) -> Path:
    enc = (src_encoding or "utf-8").lower()
    if enc in ("utf-8", "utf8", "utf-8-sig"):
        return path
    dst = path.with_suffix(path.suffix + ".utf8.tmp")
    with path.open("r", encoding=src_encoding, newline="") as src, dst.open("w", encoding="utf-8", newline="") as out:
        for line in src:
            out.write(line)
    return dst


def _read_header_raw(path: Path, encoding: str, rowskip: int = 0) -> list[str]:
    import csv
    with path.open("r", encoding=encoding, newline="") as f:
        for _ in range(rowskip):
            if f.readline() == "":
                return []
        reader = csv.reader(f)
        try:
            hdr = next(reader)
        except StopIteration:
            return []
    if hdr and isinstance(hdr[0], str) and hdr[0].startswith("\ufeff"):
        hdr[0] = hdr[0].lstrip("\ufeff")
    return hdr


def _analyze_headers(files: list[Path], encoding: str, rowskip: int) -> tuple[list[str], dict[Path, list[str]]]:
    if not files:
        return [], {}

    order: list[str] = []
    seen: set[str] = set()
    max_mult: dict[str, int] = {}
    per_file_raw: dict[Path, list[str]] = {}

    for f in files:
        hdr = _read_header_raw(f, encoding=encoding, rowskip=rowskip)
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
    DuckDB では ctid ではなく rowid を使用し、MAX(rowid) を残します。
    """
    if not pk_cols:
        return
    sql = f"""
        DELETE FROM {temp_fqtn}
        WHERE rowid NOT IN (
            SELECT MAX(rowid)
            FROM {temp_fqtn}
            GROUP BY {", ".join([f'"{c}"' for c in pk_cols])}
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _copy_df_to_table(engine: Engine, df: pd.DataFrame, table_fqtn: str, columns: list[str]):
    """DuckDB のネイティブ機能を利用して DataFrame を直接テーブルにインサート"""
    if df.empty:
        return
    df2 = df.reindex(columns=columns)
    df2 = df2.where(pd.notna(df2), None)  # DuckDB では None が NULL として扱われます

    raw_conn = engine.raw_connection()
    try:
        duck_conn = raw_conn.connection
        # DuckDBコネクションに DataFrame を一時登録して高速 INSERT
        duck_conn.register("temp_df", df2)
        duck_conn.execute(f"INSERT INTO {table_fqtn} SELECT * FROM temp_df")
        duck_conn.unregister("temp_df")
    finally:
        raw_conn.close()


def _make_temp_text_table(engine: Engine, columns: list[str]) -> str:
    """TEXT 列の TEMP TABLE を作成してテーブル名を返す。DuckDB は pg_temp 不要。"""
    temp_name = f"stg_{int(datetime.now().timestamp())}_{os.getpid()}"
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TEMP TABLE "{temp_name}" ({cols_sql});'))
    return f'"{temp_name}"'


def upsert_table(
    engine: Engine,
    table_name: str,
    cfg: dict,
    csv_root: Path,
    chunksize: int,
    auto_add_columns: bool,
):
    folder = cfg["folder"]
    pattern = cfg.get("filename_glob", "*.csv")
    pk_cols = cfg["primary_key"]
    encoding = cfg.get("encoding", "utf-8")
    rowskip = int(cfg.get("rowskip", 0))

    schema = TARGET_SCHEMA
    target_base = cfg.get("target_table", table_name)
    target_fqtn = f'"{schema}"."{target_base}"'

    src_files = _iter_csv_files(csv_root, folder, pattern)
    if not src_files:
        print(f"[{table_name}] No CSV files under {(csv_root / folder)}")
        return

    processed_files: list[Path] = []
    tmp_to_cleanup: list[Path] = []
    for f in src_files:
        g = _ensure_utf8_copy(f, encoding)
        processed_files.append(g)
        if g != f:
            tmp_to_cleanup.append(g)

    try:
        union_cols, norm_by_file = _analyze_headers(processed_files, encoding="utf-8", rowskip=rowskip)
        if not union_cols:
            print(f"[{table_name}] WARNING: header not found")
            return

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

        temp_fqtn = _make_temp_text_table(engine, db_columns)
        
        # Temp テーブルへの INDEX 作成 (DuckDBでは明示的なインデックス名が必要)
        if pk_cols:
            with engine.begin() as conn:
                idx_cols = ", ".join([f'"{c}"' for c in pk_cols])
                temp_clean_name = temp_fqtn.replace('"', '')
                conn.execute(text(f'CREATE INDEX idx_{temp_clean_name} ON {temp_fqtn} ({idx_cols});'))

        for f in processed_files:
            print(f"[{table_name}] Loading {f}")
            norm_cols = norm_by_file[f]

            for chunk in pd.read_csv(
                f,
                header=0,
                dtype=str,
                chunksize=chunksize,
                na_filter=True,
                keep_default_na=False,
                na_values=[""],
                encoding="utf-8",
                skiprows=rowskip,
            ):
                chunk.columns = norm_cols

                for pk in pk_cols:
                    if pk in chunk.columns:
                        chunk = chunk[chunk[pk].notna() & (chunk[pk] != "")]

                for c in db_columns:
                    if c not in chunk.columns:
                        chunk[c] = pd.NA
                chunk = chunk[db_columns]

                _copy_df_to_table(engine, chunk, temp_fqtn, db_columns)

        _dedupe_temp_by_pk(engine, temp_fqtn, pk_cols)

        non_key_cols = [c for c in db_columns if c not in pk_cols]
        set_clause = (
            "SET " + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in non_key_cols])
            if non_key_cols else "DO NOTHING"
        )
        
        # DuckDB の UPSERT (ON CONFLICT) 構文
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
        for p in tmp_to_cleanup:
            try:
                p.unlink()
            except Exception:
                pass


def snapshot_table_to_parquet(engine: Engine, table_name: str, out_root: Path):
    """Pandas を経由せず、DuckDB の機能で直接 Parquet に書き出すよう高速化"""
    schema = TARGET_SCHEMA
    fqtn = f'"{schema}"."{table_name}"'
    date_folder = out_root / datetime.now().strftime("%Y%m%d")
    date_folder.mkdir(parents=True, exist_ok=True)
    out_path = date_folder / f"{table_name}.parquet"

    print(f"[snapshot] {fqtn} -> {out_path}")
    with engine.begin() as conn:
        conn.execute(text(f"COPY {fqtn} TO '{out_path}' (FORMAT PARQUET)"))
    print(f"[snapshot] Wrote {out_path}")


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
        description="CSV ingestion -> DuckDB upsert (TEXT + NULL blanks) -> Parquet snapshot",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="ingest CSVs and UPSERT into DuckDB (raw TEXT)")
    p_ing.add_argument("--table", help="single table to ingest (default: all)")
    p_ing.add_argument("--chunksize", type=int, default=200_000, help="pandas read_csv chunksize (fallback)")
    p_ing.add_argument("--auto-add-columns", action="store_true",
                       help="if CSV has new columns, ALTER TABLE ADD COLUMN (TEXT)")
    p_ing.set_defaults(func=cmd_ingest)

    p_snap = sub.add_parser("snapshot", help="export tables from DuckDB to Parquet")
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