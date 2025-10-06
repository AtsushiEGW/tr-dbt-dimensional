from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion.utils import get_paths


def validate_landing(landing_root: Path) -> int:
    """
    landing 下の batch ディレクトリをざっと検査。
    返り値: 問題数
    """
    problems = 0
    for batch in landing_root.rglob("batch_id=*"):
        if not batch.is_dir():
            continue
        manifest = batch / "manifest.json"
        parts = batch / "parts"
        if not manifest.exists():
            print(f"[validate][NG] manifest missing: {manifest}")
            problems += 1
            continue
        if not parts.exists():
            print(f"[validate][NG] parts folder missing: {parts}")
            problems += 1
            continue

        try:
            meta = json.loads(manifest.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[validate][NG] manifest broken: {manifest} ({e})")
            problems += 1
            continue

        # 最小チェック：必須キー
        for key in ["namespace", "table", "run_date", "batch_id", "files"]:
            if key not in meta:
                print(f"[validate][NG] manifest key missing: {manifest} ({key})")
                problems += 1

        # parts/*.csv があるか
        csvs = list(parts.glob("*.csv")) + list(parts.glob("*.csv.gz"))
        if not csvs:
            print(f"[validate][NG] no csv files: {parts}")
            problems += 1

        print(f"[validate][OK] {batch} (files={len(csvs)})")

    return problems


def main():
    ap = argparse.ArgumentParser(description="Validate landing batches")
    ap.add_argument("--landing", help="landing root (default: PATHS)", default=None)
    args = ap.parse_args()

    paths = get_paths()
    landing_root = Path(args.landing) if args.landing else paths["LANDING_ROOT"]

    problems = validate_landing(landing_root)
    if problems:
        print(f"[validate] problems={problems}")
    else:
        print("[validate] all good")


if __name__ == "__main__":
    main()