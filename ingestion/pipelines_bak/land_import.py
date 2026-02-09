from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from ingestion.utils import get_paths, today_stamp, new_batch_id, landing_batch_dir


def _iter_files(src: Path, pattern: str) -> list[Path]:
    if not src.exists():
        raise FileNotFoundError(f"src not found: {src}")
    files = sorted(list(src.glob(pattern)), key=lambda p: str(p))
    return [p for p in files if p.is_file()]


def _md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _count_rows_csv(path: Path, encoding: str = "utf-8") -> int:
    # ヘッダ1行 + データ行N → データ行だけ数える
    try:
        with path.open("r", encoding=encoding, newline="") as f:
            first = f.readline()
            if first == "":
                return 0
            return sum(1 for _ in f)
    except UnicodeDecodeError:
        with path.open("rb") as f:
            return max(0, sum(1 for _ in f) - 1)


def import_manual(
    src: Path,
    namespace: str,
    table: str,
    run_date: str | None,
    encoding: str,
    pattern: str,
    move: bool,
    dry_run: bool,
    make_latest_symlink: bool,
):
    """
    手動でドロップした CSV を landing へ収め、manifest.json を生成する。
    """
    paths = get_paths()
    landing_root = paths["LANDING_ROOT"]

    run_date = run_date or today_stamp()
    batch_id = new_batch_id()

    csvs = _iter_files(src, pattern=pattern)
    if not csvs:
        print(f"[land-import] No files matched: {src} (pattern={pattern})")
        return

    final_dir = landing_batch_dir(landing_root, namespace, table, run_date, batch_id)
    parts_dir = final_dir / "parts"

    tmp_dir = final_dir.with_name(final_dir.name + ".tmp")
    tmp_parts = tmp_dir / "parts"

    print(f"[land-import] namespace={namespace} table={table} run_date={run_date} batch_id={batch_id}")
    print(f"[land-import] src={src} -> dst={final_dir} (move={move}, dry_run={dry_run})")

    if dry_run:
        for i, p in enumerate(csvs, 1):
            print(f"  [dry] {i:02d}: {p.name}")
        return

    tmp_parts.mkdir(parents=True, exist_ok=True)

    files_meta = []
    for p in csvs:
        dst = tmp_parts / p.name
        if move:
            shutil.move(str(p), str(dst))
        else:
            shutil.copy2(str(p), str(dst))
        meta = {
            "path": f"parts/{p.name}",
            "size": dst.stat().st_size,
            "md5": _md5(dst),
            "rows": _count_rows_csv(dst, encoding=encoding),
        }
        files_meta.append(meta)
        print(f"[land-import] {'moved' if move else 'copied'}: {p} -> {dst}")

    manifest = {
        "namespace": namespace,
        "table": table,
        "run_date": run_date,
        "batch_id": batch_id,
        "source": "manual",
        "extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "encoding": encoding,
        "files": files_meta,
        "notes": "manual drop import",
    }
    (tmp_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    final_dir.parent.mkdir(parents=True, exist_ok=True)
    if final_dir.exists():
        bak = final_dir.with_name(final_dir.name + ".bak")
        shutil.move(str(final_dir), str(bak))
    shutil.move(str(tmp_dir), str(final_dir))
    print(f"[land-import] committed: {final_dir}")

    if make_latest_symlink:
        latest = final_dir.parent / "latest"
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(final_dir.name)
        print(f"[land-import] latest -> {final_dir.name}")


def main():
    ap = argparse.ArgumentParser(description="Import manually dropped CSVs into landing with manifest.")
    ap.add_argument("--src", required=True, help="manual drop folder (contains CSVs)")
    ap.add_argument("--namespace", help="namespace (or infer from src like namespace=<ns>)")
    ap.add_argument("--table", help="table name (or infer from src like table=<table>)")
    ap.add_argument("--run-date", help="YYYYMMDD (default: today)")
    ap.add_argument("--encoding", default="utf-8", help="CSV encoding when counting rows (default: utf-8)")
    ap.add_argument("--pattern", default="*.csv", help="glob pattern (default: *.csv)")
    ap.add_argument("--move", action="store_true", help="move files instead of copy")
    ap.add_argument("--dry-run", action="store_true", help="show what would happen")
    ap.add_argument("--latest", action="store_true", help="create/refresh 'latest' symlink in run_date folder")
    args = ap.parse_args()

    # namespace/table をパスから推測（src が .../namespace=<ns>/table=<table>/ なら拾う）
    src = Path(args.src)
    namespace = args.namespace
    table = args.table
    for part in src.parts:
        if namespace is None and part.startswith("namespace="):
            namespace = part.split("=", 1)[1]
        if table is None and part.startswith("table="):
            table = part.split("=", 1)[1]
    if not namespace or not table:
        ap.error("--namespace and --table are required (or encode them in src path like namespace=<ns>/table=<table>)")

    import_manual(
        src=src,
        namespace=namespace,
        table=table,
        run_date=args.run_date,
        encoding=args.encoding,
        pattern=args.pattern,
        move=args.move,
        dry_run=args.dry_run,
        make_latest_symlink=args.latest,
    )


if __name__ == "__main__":
    main()