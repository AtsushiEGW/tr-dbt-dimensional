from __future__ import annotations

from pathlib import Path
# … 既存の import（Playwright/requests 等）

from ingestion.fetchers.utils import clean_csv_trailing_commas

def fetch_and_save():
    # 1) 自動操作で CSV をダウンロード（例: /tmp/downloads/data.csv）
    downloaded = Path("/tmp/downloads/data.csv")

    # 2) 末尾カンマをクリーン（メタ1行ある場合の例: meta_rows=1。なければ 0）
    cleaned = clean_csv_trailing_commas(
        downloaded,
        # dst_path を省略すれば *.clean.csv を作り、返り値でパスが分かる
        meta_rows=1,          # ← テーブルにより 0 / 1 を切替
        src_encoding="utf-8", # 文字コードが cp932 の場合は "cp932"
        dst_encoding="utf-8"
    )

    # 3) 保存先へ配置（運用に合わせて landing または db_ingestion を選択）
    # 例: landing へ
    landing_dir = Path("data/landing/namespace=ingest_test/table=foo/run_date=20251006/batch_id=.../parts")
    landing_dir.mkdir(parents=True, exist_ok=True)
    final_path = landing_dir / "foo_01.csv"
    cleaned.replace(final_path)

    # （あるいは db_ingestion 側）
    # dest_dir = Path("data/db_ingestion/namespace=ingest_test/table=foo")
    # dest_dir.mkdir(parents=True, exist_ok=True)
    # final_path = dest_dir / "foo_20251006_batch_id=..._foo_01.csv"
    # cleaned.replace(final_path)

    return final_path