

playwright で自動的にダウンロードした csv ファイルは以下の関数で landing に保存する


```py
# ingestion/fetchers/site_name_playwright.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from ingestion.utils import (
    get_paths,
    today_stamp,
    new_batch_id,
    landing_batch_dir,
)


def _md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _count_rows_csv(path: Path, encoding: str = "utf-8") -> int:
    """先頭1行をヘッダとみなし、データ行数を返す（巨大ファイル向けの軽量カウント）。"""
    try:
        with path.open("r", encoding=encoding, newline="") as f:
            first = f.readline()
            if first == "":
                return 0
            return sum(1 for _ in f)
    except UnicodeDecodeError:
        # 文字化けする場合はバイナリ行数-1（ヘッダ分）で概算
        with path.open("rb") as f:
            return max(0, sum(1 for _ in f) - 1)


def persist_downloads_to_landing(
    downloaded_files: Iterable[Path],
    *,
    namespace: str,
    table: str,
    run_date: str | None = None,      # 省略時は today (YYYYMMDD)
    encoding: str = "utf-8",          # 行数カウントに使用
    move: bool = True,                # True: 移動 / False: コピー
    make_latest_symlink: bool = True, # run_date フォルダ直下に latest シンボリックを張る
    notes: str | None = None,         # manifest に残すメモ
) -> Path:
    """
    Playwright で取得した CSV 群を landing へ保存し、manifest.json を生成する。

    戻り値: 提交された landing の batch ディレクトリ（…/run_date=YYYYMMDD/batch_id=...）
    """
    files = [Path(p) for p in downloaded_files if Path(p).is_file()]
    if not files:
        raise ValueError("persist_downloads_to_landing: no files to persist")

    paths = get_paths()
    landing_root = paths["LANDING_ROOT"]

    run_date = run_date or today_stamp()
    batch_id = new_batch_id()

    final_dir = landing_batch_dir(landing_root, namespace, table, run_date, batch_id)
    tmp_dir = final_dir.with_name(final_dir.name + ".tmp")
    parts_dir = tmp_dir / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)

    files_meta = []
    for src in sorted(files, key=lambda p: str(p)):
        dst = parts_dir / src.name
        if move:
            # ダウンロードフォルダから landing へ移動
            dst.write_bytes(src.read_bytes())
            src.unlink(missing_ok=True)
        else:
            # 監査の都合などで原本を残したいときはコピー
            dst.write_bytes(src.read_bytes())

        meta = {
            "path": f"parts/{dst.name}",
            "size": dst.stat().st_size,
            "md5": _md5(dst),
            "rows": _count_rows_csv(dst, encoding=encoding),
        }
        files_meta.append(meta)

    manifest = {
        "namespace": namespace,
        "table": table,
        "run_date": run_date,
        "batch_id": batch_id,
        "source": "playwright",
        "extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "encoding": encoding,
        "files": files_meta,
        "notes": notes or "downloaded via playwright",
    }
    (tmp_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # アトミックに ".tmp" → 本番ディレクトリへコミット
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    if final_dir.exists():
        backup = final_dir.with_name(final_dir.name + ".bak")
        if backup.exists():
            # 古い .bak が残っていたら消しておく（運用方針次第）
            if backup.is_dir():
                for p in backup.rglob("*"):
                    p.unlink(missing_ok=True)
                backup.rmdir()
            else:
                backup.unlink(missing_ok=True)
        final_dir.rename(backup)
    tmp_dir.rename(final_dir)

    # latest シンボリック（運用を楽にするオプション）
    if make_latest_symlink:
        latest = final_dir.parent / "latest"
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        # relative symlink（ディレクトリ名に対するリンク）
        latest.symlink_to(final_dir.name)

    return final_dir


```


```py
from pathlib import Path
from playwright.sync_api import sync_playwright

# 上で定義した関数を import
from ingestion.fetchers.site_name_playwright import persist_downloads_to_landing

def fetch_and_land(namespace: str, table: str):
    download_dir = Path("./tmp/downloads")  # Playwright のダウンロード先
    download_dir.mkdir(parents=True, exist_ok=True)

    downloaded: list[Path] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 例のサイトへアクセスしてダウンロード実行
        page.goto("https://example.com/report")
        with page.expect_download() as dl_info:
            page.click("text=CSVダウンロード")
        dl = dl_info.value
        # 保存先を指定
        saved_path = Path(dl.path())  # 一時パス
        final_path = download_dir / dl.suggested_filename
        final_path.write_bytes(Path(saved_path).read_bytes())
        downloaded.append(final_path)

        # 複数ボタン/複数ファイルのケースでも downloaded に足していけばOK

        context.close()
        browser.close()

    # まとめて landing へコミット（manifest 生成・latest シンボリック作成）
    batch_dir = persist_downloads_to_landing(
        downloaded_files=downloaded,
        namespace=namespace,
        table=table,
        run_date=None,       # 省略で今日
        encoding="utf-8",    # 行数カウントのための想定エンコーディング
        move=True,           # True: manual_drop側に原本を残さない
        make_latest_symlink=True,
        notes="site_name automated fetch",
    )
    print(f"landed: {batch_dir}")

```


## 補足（設計の意図）
- landing 直行：Playwright で取得直後に landing へ格納→manifest.json を作ることで、いつ/何を取り込んだかが追跡可能に。
- アトミックコミット：まず *.tmp にファイルと manifest を作り、最後に rename で一気に本番化。中途半端な状態で他プロセスに見えないようにするためです。
- latest シンボリック：後段の promote / ingest が「最新バッチ」を簡単に選べるようにするお約束。既存の promote や ingest_flow とも相性◎。
- md5/rows：監査・検証用の軽メタデータ。必要になれば列数/サンプル行のダイジェストなども追加できます。

これで「ダウンロード → landing 保存 →（必要なら）make ingest-one で Postgres upsert & snapshot」までの運用がスムーズに繋がります。