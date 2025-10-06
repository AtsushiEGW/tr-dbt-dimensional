以下は、あなたの現在のリポジトリ構成＆Makefile前提での「CSV→landing→db_ingestion→Postgres（UPSERT）→Parquet」までの**運用手順（ステップバイステップ）**です。
実際に使うコマンドはそのままコピペできます。

⸻

前提（最初に一度だけ）
	1.	Python 実行を “モジュール” 方式にしている
Makefile は python -m ingestion.pipelines.* を使います。ingestion がパッケージ扱いになるよう、プロジェクトルートで実行してください。
（必要なら export PYTHONPATH="." を .env やシェルに設定）
	2.	.env を用意
環境変数（Postgres 接続、ルートフォルダなど）を .env に記述済みで、Makefileが include .env しています。
最低限:

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin12345
POSTGRES_DB=mypostgres
TARGET_SCHEMA=raw

DATA_DIR=./data
CSV_ROOT=./data/db_ingestion
PARQUET_ROOT=./data/parquet
ARCHIVE_ROOT=./data/archive
LANDING_ROOT=./data/landing

	3.	テーブル定義（ingestion/config/tables.yml）

	•	folder: namespace=<ns>/table=<table> を必ず合わせる
	•	primary_key（単一／複合）を必ず正しく定義
	•	文字コードが Shift_JIS のテーブルは encoding: shift_jis を設定

⸻

1. 原本CSVを landing に取り込む

A) 手動で落としてきた CSV を landing に登録（manifest 付き）

目的: namespace=<ns>/table=<table>/run_date=<YYYYMMDD>/batch_id=<...>/parts/*.csv という厳密な階層へ移動またはコピーし、manifest.json を生成して完全性を担保します。

# movies テーブル（ingest_test 名前空間）の例
make land-import-movies \
  SRC=path/to/manual_drop/namespace=ingest_test/table=movies \
  NAMESPACE=ingest_test \
  RUN_DATE=20251005 \
  MOVE=1

	•	SRC … 取り込み元フォルダ（*.csv が置いてある場所）
	•	NAMESPACE … 取り込み先の namespace
	•	RUN_DATE … 実行日パーティション（省略時は今日）
	•	MOVE=1 … コピーではなく移動（未指定ならコピー）
	•	実行後: data/landing/namespace=ingest_test/table=movies/run_date=20251005/batch_id=.../parts/*.csv が作られる
	•	シンボリックリンク latest が run_date 直下に作られます

まずは ドライランで確認したい場合:

make land-import-movies SRC=... NAMESPACE=ingest_test RUN_DATE=20251005 DRY_RUN=1

B) Playwright 等で自動取得したCSV

取得スクリプトの最後で landing 階層へ 直接書き込む か、上の land-import-* を呼び出すのが安心です（manifest 自動生成の恩恵を受けられます）。

⸻

2. landing の軽い検証（任意だが推奨）

make validate

	•	manifest.json の存在、parts/*.csv の存在などをチェック
	•	問題があれば、メッセージに従って修正してください

⸻

3. 採用バッチを db_ingestion にプロモート

目的: landing の「どの run_date / どの batch を採用するか」を決め、db_ingestion に平置きする（ファイル名に run_date / batch_id を付けて衝突回避）。

# 最新バッチを movies にプロモート
make promote-movies NAMESPACE=ingest_test DATE=20251005 BATCH=latest

	•	NAMESPACE … 名前空間
	•	DATE … run_date（YYYYMMDD）
	•	BATCH … latest か具体的ID（batch_id=... の末尾ID文字列）
	•	例のエラー:

FileNotFoundError: No batch under data/landing/namespace=.../table=.../run_date=20251005

は「指定 run_date にバッチが無い」ことを意味します。
ls -R data/landing/namespace=.../table=... で run_date と batch_id を確認してください。

⸻

4. Postgres へ UPSERT（CSV → TEMP → 後勝ち重複除去 → ON CONFLICT）

全テーブル

make ingest

単一テーブル

make ingest-movies

	•	tables.yml の primary_key に基づき、TEMP テーブル内で主キー重複を 後勝ち（ctid） で 1件に正規化してから UPSERT します。
	•	CSVの空欄は本物の NULL として取り込まれます（dbt で型変換する前提）。
	•	CSV側に新しい列が出た場合、--auto-add-columns で TEXT 列を自動追加します（Makefile 既定で有効）。

Shift_JIS のテーブルは tables.yml に encoding: shift_jis を指定してください。
PK 欠損行は取り込み前に除外しています（UPSERTの衝突源になるため）。

⸻

5. スナップショット（DB → Parquet）

目的: 再構築やバックアップ用途。毎回DBの全件を Parquet に吐き出します（DATA_DIR/parquet/YYYYMMDD/table.parquet）。

	•	全テーブル:

make snapshot

	•	単一テーブル:

make snapshot-movies


⸻

6. 古い CSV を整理（db_ingestion 配下）
	•	削除対象だけ確認（ドライラン）

make clean-dry

	•	アーカイブへ移動

make clean-archive

	•	完全削除

make clean-hard

保持日数は .env の RETENTION_DAYS（例: 45）で設定。

⸻

7. landing の運用クリーン（圧縮・削除）

LANDING_COMPRESS_AFTER_DAYS を過ぎたバッチは parts/*.csv.gz に圧縮、
LANDING_RETENTION_DAYS を過ぎたバッチは削除（ただし LANDING_KEEP_PER_NAMESPACE で直近N件は保護）。

make clean-landing


⸻

まとめ：日次運用フロー例
	1.	CSVを landing に入れる
	•	手動: make land-import-<table> SRC=... NAMESPACE=... RUN_DATE=... MOVE=1
	•	自動: スクレイパが landing 直書き or land-import 呼び出し
	2.	検証
	•	make validate
	3.	採用バッチをプロモート
	•	make promote-<table> NAMESPACE=... DATE=... BATCH=latest
	4.	UPSERT（DB反映）
	•	テーブル単体: make ingest-<table>
	•	すべて: make ingest
	5.	Parquet スナップショット
	•	make snapshot（または make snapshot-<table>）
	6.	整理
	•	make clean-landing
	•	make clean-dry → 問題なければ make clean-archive もしくは make clean-hard

⸻

トラブルシュート（よくある）
	•	No batch under ...
→ 指定した DATE に該当バッチがない。ls -R data/landing/namespace=<ns>/table=<table> で run_date と batch_id を確認し、正しい値で promote を実行。
	•	ModuleNotFoundError: No module named 'ingestion'
→ ルートで python -m ingestion... を実行していない／PYTHONPATH が不正。
ルートディレクトリで実行 or export PYTHONPATH="."。
	•	CardinalityViolation: ON CONFLICT DO UPDATE ...
→ TEMP 内に同一 PK が複数行ある状態で UPSERT した可能性。
本フローは _dedupe_temp_by_pk() で後勝ちに揃え済み（最新版コードを使用）。
それでも出る場合は PK 定義ミスや CSV 側の列名ズレを疑う。
	•	文字化けや UnicodeDecodeError
→ tables.yml の encoding を対象テーブルに設定（例: shift_jis）。
BOM 付きUTF-8（UTF-8 SIG）も _read_header_raw で除去済み。
	•	新しい列が取り込まれない
→ デフォルトでは無視。--auto-add-columns（Makefile で既定ON）で TEXT 列として自動追加。

⸻

ログの場所
	•	ingestion/logs/*.log に各ターゲット別のログを追記します（tee -a）。
エラー時はここを確認すれば、どのステップで落ちたかがすぐ分かります。

⸻

この手順のまま回せば、**冪等（同じCSVを何度取り込んでも結果が安定）**で、スキーマ追加にも自動追従し、Parquetバックアップで再構築容易なパイプラインとして運用できます。必要に応じて、make all（validate → ingest → snapshot）などのショートカットターゲットを追加するのもおすすめです。