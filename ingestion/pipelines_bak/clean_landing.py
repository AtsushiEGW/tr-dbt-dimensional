from __future__ import annotations

import gzip
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from ingestion.utils import get_paths


def _parse_run_date(dirpath: Path) -> datetime | None:
    # .../run_date=YYYYMMDD/
    for part in reversed(dirpath.parts):
        if part.startswith("run_date="):
            try:
                return datetime.strptime(part.split("=", 1)[1], "%Y%m%d")
            except ValueError:
                return None
    return None


def _compress_csv_in_parts(parts_dir: Path):
    for csv in list(parts_dir.glob("*.csv")):
        gz = csv.with_suffix(csv.suffix + ".gz")  # .csv.gz
        if gz.exists():
            continue
        with csv.open("rb") as src, gzip.open(gz, "wb") as dst:
            shutil.copyfileobj(src, dst)
        csv.unlink()
        print(f"[landing-clean] compressed: {csv.name} -> {gz.name}")


def clean_landing():
    paths = get_paths()
    landing = paths["LANDING_ROOT"]

    retention_days = int(os.getenv("LANDING_RETENTION_DAYS", "90"))
    compress_after_days = int(os.getenv("LANDING_COMPRESS_AFTER_DAYS", "60"))
    keep_per_namespace = int(os.getenv("LANDING_KEEP_PER_NAMESPACE", "3"))

    now = datetime.now()
    cutoff_delete = now - timedelta(days=retention_days)
    cutoff_compress = now - timedelta(days=compress_after_days)

    # namespace/table ごとに直近 keep_per_namespace を保護
    groups: dict[str, list[Path]] = {}
    for batch in landing.rglob("batch_id=*"):
        if not batch.is_dir():
            continue
        ns = "<unknown>"
        tbl = "<unknown>"
        for part in batch.parts:
            if part.startswith("namespace="):
                ns = part.split("=", 1)[1]
            if part.startswith("table="):
                tbl = part.split("=", 1)[1]
        key = f"{ns}/{tbl}"
        groups.setdefault(key, []).append(batch)

    for key, batches in groups.items():
        batches.sort(key=lambda p: str(p))  # batch_id 命名が時系列ソート前提
        protected = set(batches[-keep_per_namespace:]) if keep_per_namespace > 0 else set()

        for b in batches:
            run_dt = _parse_run_date(b)
            if run_dt is None:
                print(f"[landing-clean] skip (no run_date): {b}")
                continue

            parts_dir = b / "parts"
            if parts_dir.exists() and run_dt < cutoff_compress:
                _compress_csv_in_parts(parts_dir)

            if b in protected:
                print(f"[landing-clean] protect latest: {b}")
                continue

            if run_dt < cutoff_delete:
                shutil.rmtree(b)
                print(f"[landing-clean] deleted: {b}")


if __name__ == "__main__":
    clean_landing()