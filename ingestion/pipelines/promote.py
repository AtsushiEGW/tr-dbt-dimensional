from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from ingestion.utils import get_paths


def resolve_batch_dir(landing_root: Path, namespace: str, table: str, run_date: str, batch_id: str | None) -> Path:
    base = landing_root / f"namespace={namespace}" / f"table={table}" / f"run_date={run_date}"
    if batch_id in (None, "latest"):
        candidates = sorted([p for p in base.glob("batch_id=*") if p.is_dir()], key=lambda p: str(p))
        if not candidates:
            raise FileNotFoundError(f"No batch under {base}")
        return candidates[-1]
    else:
        p = base / f"batch_id={batch_id}"
        if not p.exists():
            raise FileNotFoundError(f"Batch not found: {p}")
        return p


def promote(args):
    paths = get_paths()
    landing_root = paths["LANDING_ROOT"]
    csv_root = paths["CSV_ROOT"]

    batch_dir = resolve_batch_dir(landing_root, args.namespace, args.table, args.run_date, args.batch_id)
    parts_dir = batch_dir / "parts"
    if not parts_dir.exists():
        raise FileNotFoundError(f"parts not found: {parts_dir}")

    dest_dir = csv_root / f"namespace={args.namespace}" / f"table={args.table}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src in sorted(parts_dir.glob("*.csv"), key=lambda p: str(p)):
        # 衝突しないように run_date/batch_id をファイル名に付与
        dest = dest_dir / f"{args.table}_{args.run_date}_{batch_dir.name}_{src.name}"
        shutil.copy2(src, dest)
        copied += 1
        print(f"[promote] {src} -> {dest}")

    print(f"[promote] copied={copied}, to={dest_dir}")


def main():
    ap = argparse.ArgumentParser(description="Promote landing batch -> db_ingestion")
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--run-date", required=True, help="YYYYMMDD")
    ap.add_argument("--batch-id", default="latest", help="specific id or 'latest'")
    args = ap.parse_args()
    promote(args)


if __name__ == "__main__":
    main()