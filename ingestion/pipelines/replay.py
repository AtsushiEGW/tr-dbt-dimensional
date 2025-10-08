# ingestion/pipelines/replay.py
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable

from ingestion.utils import get_paths
from ingestion.pipelines.csv_to_db import (
    load_config,
    upsert_table,
)
# promote と同じ振る舞いを内蔵（コピーのみ。1テーブルに全バッチを集約→最後に1回だけUPSERT）

def _iter_batches(landing_root: Path, namespace: str, table: str) -> list[Path]:
    """
    landing/namespace=<ns>/table=<table>/run_date=YYYYMMDD/batch_id=... を
    run_date → batch_id の辞書順（= 時系列昇順）で返す
    """
    base = landing_root / f"namespace={namespace}" / f"table={table}"
    run_dirs = [p for p in base.glob("run_date=*") if p.is_dir()]
    batches: list[Path] = []
    for r in sorted(run_dirs, key=lambda p: p.name):
        bs = [p for p in r.glob("batch_id=*") if p.is_dir()]
        bs.sort(key=lambda p: p.name)
        batches.extend(bs)
    return batches

def _copy_batch_parts_to_csv_root(batch_dir: Path, csv_root: Path, namespace: str, table: str, run_date: str):
    """
    promote.py と同様：parts/*.csv を db_ingestion 側へコピー。
    ファイル名は <table>_<run_date>_<batch_dir.name>_<元ファイル名>
    """
    parts_dir = batch_dir / "parts"
    if not parts_dir.exists():
        raise FileNotFoundError(f"parts not found: {parts_dir}")

    dest_dir = csv_root / f"namespace={namespace}" / f"table={table}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src in sorted(parts_dir.glob("*.csv"), key=lambda p: str(p)):
        dest = dest_dir / f"{table}_{run_date}_{batch_dir.name}_{src.name}"
        shutil.copy2(src, dest)
        copied += 1
        print(f"[replay][copy] {src} -> {dest}")
    return copied

def _wipe_table_folder(csv_root: Path, namespace: str, table: str):
    """
    そのテーブルの db_ingestion フォルダを空にする（リプレイの開始前にきれいにする）
    """
    dest_dir = csv_root / f"namespace={namespace}" / f"table={table}"
    if dest_dir.exists():
        for p in dest_dir.glob("*"):
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
        print(f"[replay] wiped: {dest_dir}")
    else:
        dest_dir.mkdir(parents=True, exist_ok=True)

def _discover_namespaces_tables(landing_root: Path) -> dict[str, set[str]]:
    """
    landing 直下から namespace と table を列挙
    戻り値: { namespace: {table, ...}, ... }
    """
    result: dict[str, set[str]] = {}
    for ns_dir in landing_root.glob("namespace=*"):
        if not ns_dir.is_dir():
            continue
        ns = ns_dir.name.split("=", 1)[1]
        for tbl_dir in ns_dir.glob("table=*"):
            if not tbl_dir.is_dir():
                continue
            table = tbl_dir.name.split("=", 1)[1]
            result.setdefault(ns, set()).add(table)
    return result

def replay(namespace: str | None, table: str | None, since: str | None, snapshot: bool):
    paths = get_paths()
    landing_root = paths["LANDING_ROOT"]
    csv_root = paths["CSV_ROOT"]

    cfg = load_config()
    tables_cfg = cfg["tables"]

    # 対象 namespace/table を決定
    targets: list[tuple[str, str]] = []
    if namespace and table:
        targets = [(namespace, table)]
    else:
        ns_tables = _discover_namespaces_tables(landing_root)
        for ns, tbls in ns_tables.items():
            if namespace and ns != namespace:
                continue
            for tb in sorted(tbls):
                if table and tb != table:
                    continue
                targets.append((ns, tb))

    if not targets:
        print("[replay] no targets found under landing. (check --namespace/--table)")
        return

    # テーブル単位で：db_ingestion 側のフォルダを空に → 全バッチを時系列でコピー → 1回だけ upsert
    from ingestion.pipelines.csv_to_db import get_engine, ensure_schema, TARGET_SCHEMA, snapshot_table_to_parquet
    engine = get_engine()
    ensure_schema(engine, TARGET_SCHEMA)

    for ns, tb in targets:
        if tb not in tables_cfg:
            print(f"[replay][warn] table '{tb}' not in tables.yml. skip.")
            continue
        spec = tables_cfg[tb]

        print(f"[replay] ========== {ns}/{tb} ==========")
        _wipe_table_folder(csv_root, ns, tb)

        # バッチの時系列列挙
        batches = _iter_batches(landing_root, ns, tb)
        if since:
            # run_date=YYYYMMDD 文字列でフィルタ
            batches = [b for b in batches if b.parent.name >= f"run_date={since}"]

        if not batches:
            print(f"[replay] no batches for {ns}/{tb}")
            continue

        total_files = 0
        for b in batches:
            run_date = b.parent.name.split("=", 1)[1]
            total_files += _copy_batch_parts_to_csv_root(b, csv_root, ns, tb, run_date)

        print(f"[replay] copied files: {total_files} for {ns}/{tb}")

        # 1回だけ UPSERT（db_ingestion/namespace=<ns>/table=<tb> 配下の全CSVが対象）
        upsert_table(
            engine=engine,
            table_name=tb,
            cfg=spec,
            csv_root=csv_root,
            chunksize=spec.get("chunksize", 200_000),
            auto_add_columns=True,
        )

        if snapshot:
            snapshot_table_to_parquet(engine, spec.get("target_table", tb), paths["PARQUET_ROOT"])

def main():
    ap = argparse.ArgumentParser(description="Replay landing -> db_ingestion -> upsert (chronological).")
    ap.add_argument("--namespace", help="target namespace (default: all under landing)")
    ap.add_argument("--table", help="target table (default: all tables in namespace)")
    ap.add_argument("--since", help="YYYYMMDD; only batches on/after this run_date")
    ap.add_argument("--snapshot", action="store_true", help="emit parquet snapshot after each table upsert")
    args = ap.parse_args()
    replay(args.namespace, args.table, args.since, args.snapshot)

if __name__ == "__main__":
    main()