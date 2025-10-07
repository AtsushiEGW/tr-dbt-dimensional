from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from ingestion.utils import get_paths, get_engine, ensure_schema
from ingestion.pipelines.land_import import import_manual
from ingestion.pipelines.promote import resolve_batch_dir
from ingestion.pipelines.validate import validate_landing
from ingestion.pipelines.csv_to_db import (
    load_config,
    upsert_table,
    snapshot_table_to_parquet,
    TARGET_SCHEMA,
)

def _has_csv(p: Path, patterns: Iterable[str] = ("*.csv",)) -> bool:
    for pat in patterns:
        if any(p.glob(pat)):
            return True
    return False

def run_one(
    namespace: str,
    table: str,
    src: Path | None = None,
    run_date: str | None = None,
    encoding: str = "utf-8",
    pattern: str = "*.csv",
    move: bool = True,
    dry_run: bool = False,
    auto_add_columns: bool = True,
    chunksize: int | None = None,
):
    """
    manual_drop から landing 取り込み → validate → promote → UPSERT → snapshot を1発で。
    """
    paths = get_paths()
    engine = get_engine()
    ensure_schema(engine, TARGET_SCHEMA)

    # 1) manual_drop の推定（未指定なら既定パス）
    if src is None:
        src = paths["LANDING_ROOT"].parent / "manual_drop" / f"namespace={namespace}" / f"table={table}"

    # 2) landing へ取り込み（manifest作成＆latestシンボリック付与）
    import_manual(
        src=src,
        namespace=namespace,
        table=table,
        run_date=run_date,
        encoding=encoding,
        pattern=pattern,
        move=move,
        dry_run=dry_run,
        make_latest_symlink=True,
    )
    if dry_run:
        print("[ingest-flow] dry-run: stop after land-import")
        return

    # 3) validate（軽検査）。問題があれば中断
    problems = validate_landing(get_paths()["LANDING_ROOT"])
    if problems:
        raise SystemExit(f"[ingest-flow] landing validation failed (problems={problems})")

    # 4) promote: landing の “latest” を db_ingestion へコピー
    #    latest を解決（run_date 必須）。run_date未指定の場合は import_manual が today を使うため、
    #    最新 run_date を推測する
    landing_root = paths["LANDING_ROOT"]
    ns_dir = landing_root / f"namespace={namespace}" / f"table={table}"
    if run_date is None:
        # namespace/table 配下の run_date=YYYYMMDD をソートし最後を採用
        run_dirs = sorted([p for p in ns_dir.glob("run_date=*") if p.is_dir()], key=lambda p: str(p))
        if not run_dirs:
            raise FileNotFoundError(f"[ingest-flow] no run_date dir under {ns_dir}")
        run_date = run_dirs[-1].name.split("=", 1)[1]

    batch_dir = resolve_batch_dir(landing_root, namespace, table, run_date, "latest")
    parts_dir = batch_dir / "parts"
    if not parts_dir.exists():
        raise FileNotFoundError(f"[ingest-flow] parts not found: {parts_dir}")

    csv_root = paths["CSV_ROOT"]
    dest_dir = csv_root / f"namespace={namespace}" / f"table={table}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src_file in sorted(parts_dir.glob("*.csv"), key=lambda p: str(p)):
        dest = dest_dir / f"{table}_{run_date}_{batch_dir.name}_{src_file.name}"
        dest.write_bytes(src_file.read_bytes())
        copied += 1
        print(f"[ingest-flow][promote] {src_file} -> {dest}")
    print(f"[ingest-flow][promote] copied={copied}, to={dest_dir}")

    # 5) UPSERT（テーブル単位）
    cfg = load_config()
    spec = cfg["tables"][table]  # tables.yml に必須
    if chunksize is None:
        chunksize = spec.get("chunksize", 200_000)

    upsert_table(
        engine=engine,
        table_name=table,
        cfg=spec,
        csv_root=csv_root,
        chunksize=chunksize,
        auto_add_columns=auto_add_columns,
    )

    # 6) snapshot（対象テーブルのみ）
    snapshot_table_to_parquet(engine, spec.get("target_table", table), paths["PARQUET_ROOT"])
    print("[ingest-flow] DONE")

def run_auto(
    encoding: str = "utf-8",
    pattern: str = "*.csv",
    move: bool = True,
    dry_run: bool = False,
    auto_add_columns: bool = True,
):
    """
    manual_drop 以下の namespace=*/table=* で、CSVがある場所だけを自動検出し、順に run_one 実行。
    """
    paths = get_paths()
    manual_root = paths["LANDING_ROOT"].parent / "manual_drop"

    pairs: list[tuple[str, str, Path]] = []
    for ns_dir in manual_root.glob("namespace=*"):
        if not ns_dir.is_dir():
            continue
        namespace = ns_dir.name.split("=", 1)[1]
        for tbl_dir in ns_dir.glob("table=*"):
            if not tbl_dir.is_dir():
                continue
            table = tbl_dir.name.split("=", 1)[1]
            if _has_csv(tbl_dir, (pattern,)):
                pairs.append((namespace, table, tbl_dir))

    if not pairs:
        print("[ingest-flow][auto] nothing to ingest under manual_drop")
        return

    for namespace, table, src in pairs:
        print(f"[ingest-flow][auto] start: {namespace}.{table} (src={src})")
        try:
            run_one(
                namespace=namespace,
                table=table,
                src=src,
                run_date=None,         # today
                encoding=encoding,
                pattern=pattern,
                move=move,
                dry_run=dry_run,
                auto_add_columns=auto_add_columns,
                chunksize=None,
            )
        except SystemExit as e:
            print(f"[ingest-flow][auto] aborted for {namespace}.{table}: {e}")
        except Exception as e:
            print(f"[ingest-flow][auto] error for {namespace}.{table}: {e}")

def main():
    ap = argparse.ArgumentParser(description="One-shot flow: manual_drop -> landing -> promote -> upsert -> snapshot")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("one", help="ingest single namespace.table")
    p1.add_argument("--namespace", required=True)
    p1.add_argument("--table", required=True)
    p1.add_argument("--src", help="manual_drop folder (default: manual_drop/namespace=.../table=...)")
    p1.add_argument("--run-date", help="YYYYMMDD (default: today)")
    p1.add_argument("--encoding", default="utf-8")
    p1.add_argument("--pattern", default="*.csv")
    p1.add_argument("--no-move", action="store_true", help="copy (default: move)")
    p1.add_argument("--dry-run", action="store_true")
    p1.add_argument("--no-auto-add-columns", action="store_true")
    p1.add_argument("--chunksize", type=int, help="override chunksize")

    p2 = sub.add_parser("auto", help="ingest all namespace/table under manual_drop")
    p2.add_argument("--encoding", default="utf-8")
    p2.add_argument("--pattern", default="*.csv")
    p2.add_argument("--no-move", action="store_true")
    p2.add_argument("--dry-run", action="store_true")
    p2.add_argument("--no-auto-add-columns", action="store_true")

    args = ap.parse_args()

    if args.cmd == "one":
        run_one(
            namespace=args.namespace,
            table=args.table,
            src=Path(args.src) if args.src else None,
            run_date=args.run_date,
            encoding=args.encoding,
            pattern=args.pattern,
            move=not args.no_move,
            dry_run=args.dry_run,
            auto_add_columns=not args.no_auto_add_columns,
            chunksize=args.chunksize,
        )
    else:
        run_auto(
            encoding=args.encoding,
            pattern=args.pattern,
            move=not args.no_move,
            dry_run=args.dry_run,
            auto_add_columns=not args.no_auto_add_columns,
        )

if __name__ == "__main__":
    main()