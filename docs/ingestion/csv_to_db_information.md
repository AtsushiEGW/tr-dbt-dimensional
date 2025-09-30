データ取り込みパイプライン ドキュメント（postgres × dbt 前段 / CSV→UPSERT→Parquet）

このドキュメントは、以下の 2 つのモジュールについて関数ごとの解説と、実運用時の実行方法（コマンド例・運用フロー）をまとめたものです。
	•	ingestion/csv_to_db.py … CSV 群 → Postgres（raw TEXT）へ UPSERT、Parquet スナップショット、CSV クリーンアップ
	•	utils.py … 共有ユーティリティ（接続、スキーマ作成、テーブル検査、列追加、パス管理 など）

⸻

0. 前提

ディレクトリ構成（抜粋）

.
|-- data/
|   |-- csvs_demo/
|   |   |-- <table-a>/*.csv
|   |   `-- <table-b>/*.csv
|   `-- parquet/           # snapshot 保存先
|-- ingestion/
|   |-- csv_to_db.py
|   `-- config/tables.yml  # 取り込み対象の定義
|-- utils.py
|-- .env / .env.prod       # 環境変数（DB, パス 等）

主要な環境変数

変数名	意味	例
PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE	Postgres 接続情報	localhost / 5432 / …
TARGET_SCHEMA	取り込み先のスキーマ	raw（既定）
DATA_DIR	データルート（省略時 ./data）	/mnt/data
CSV_ROOT	CSV のルート	${DATA_DIR}/csvs_demo
PARQUET_ROOT	Parquet 出力ルート	${DATA_DIR}/parquet
ARCHIVE_ROOT	CSV アーカイブルート	${DATA_DIR}/archive
RETENTION_DAYS	CSV の保存日数	60 など

.env / .env.prod に保存。get_env() が “未設定ならフォールバック・必須なら例外” の挙動で利用します。

ingestion/config/tables.yml の例

tables:
  movies:
    folder: movies                 # CSV_ROOT/movies/*.csv
    filename_glob: "*.csv"         # 省略可（デフォルト *.csv）
    primary_key: ["movieId"]       # 複合キー可
    target_table: movies_raw       # 省略可（デフォルトはキー名と同じ）
  ratings:
    folder: ratings
    primary_key: ["userId", "movieId"]


⸻

1. utils.py の関数解説

get_engine() -> Engine
	•	役割: SQLAlchemy の Engine を生成（接続プールの入口）。
	•	内部: DSN（URL）を構築し create_engine(url, pool_pre_ping=True)。
	•	ポイント: pool_pre_ping=True により、長時間アイドル後の “切れた接続” を使う直前にヘルスチェックして再接続するため、バッチで安定。

ensure_schema(engine: Engine, schema: str) -> None
	•	役割: スキーマの存在保証（なければ作成）。
	•	実装: CREATE SCHEMA IF NOT EXISTS "schema" をトランザクションで実行。
	•	意図: 初回や新環境での失敗を防ぐ。冪等。

get_paths() -> dict[str, Path]
	•	役割: CSV/Parquet/Archive のパスを一元管理。
	•	実装: 環境変数（あれば優先）→ なければ DATA_DIR 配下の既定にフォールバック。
	•	戻り: {"CSV_ROOT": Path(...), "PARQUET_ROOT": Path(...), "ARCHIVE_ROOT": Path(...)}。

get_retention_days() -> int
	•	役割: 古い CSV の保持日数を取得。
	•	実装: RETENTION_DAYS（未設定なら適切なデフォルト）。
	•	用途: clean_old_csvs() で使用。

today_stamp() -> str
	•	役割: 日付スタンプを文字列で返す（例：YYYYMMDD）。
	•	用途: スナップショットのフォルダ名などに利用。

table_exists(engine: Engine, schema: str, table: str) -> bool
	•	役割: テーブル存在チェック。
	•	実装: information_schema.tables を参照（必要なら table_type='BASE TABLE' を追加）。
	•	注意: TOCTOUは理論上回避不能なので、作成側は IF NOT EXISTS / エラー握りつぶし併用が安全。

get_table_columns(engine: Engine, schema: str, table: str) -> list[str]
	•	役割: 列名を定義順で取得。
	•	実装: information_schema.columns を ORDER BY ordinal_position。
	•	使い所: TEMPテーブルや COPY の “列順” を DB に合わせるため。

create_text_table(engine: Engine, schema: str, table: str, columns: list[str], pk_cols: list[str]) -> None
	•	役割: TEXT 列だけの raw テーブルを作成（主キー付き・複合可）。
	•	意図: raw 層では「型変換は dbt に委譲」。取り込みの柔軟性とシンプルさを優先。

add_missing_text_columns(engine: Engine, schema: str, table: str, missing_cols: list[str]) -> None
	•	役割: CSV にはあるが DB にない列を TEXT で追加。
	•	実装: ALTER TABLE ... ADD COLUMN "<col>" TEXT をトランザクションでまとめて実行。
	•	注意: 名前のバリデーション（英数+アンダースコアなど）を行い、外部入力を直に埋め込まない。

⸻

2. ingestion/csv_to_db.py の関数解説

定数・基本
	•	HERE / CFG_PATH … スクリプト自身のパスから config の場所を解決。
	•	TARGET_SCHEMA … 取り込み先のスキーマ（環境変数可）。
	•	PATHS … utils.get_paths() の戻り（Path dict）。

load_config() -> dict
	•	役割: config/tables.yml を読み、tables セクションを返す。
	•	用途: CLI の --table 指定や対象一覧取得に使用。

_iter_csv_files(folder_root: Path, folder: str, pattern: str) -> list[Path]
	•	役割: folder_root/folder 配下で pattern に一致する CSV を列挙し、文字列順に整列。
	•	注意: 再帰検索ではない（必要なら rglob）。

_union_csv_headers(files: list[Path]) -> tuple[list[str], dict[Path, list[str]], dict[str, int]]
	•	役割: 重複カラム名対応を含むヘッダ正規化。
	•	全 CSV の**“生ヘッダ”**を読み、同名列の最大多重度を計算（例：qty が最大 3 本）。
	•	多重度が 2 以上の列は、出現順に name_1, name_2, ... に正規化。単独出現のファイルも系列の _1 に寄せる（横断一貫性）。
	•	正規化後の列を、最初のファイルの順序を基準に和集合化（新規は末尾に追加）。
	•	戻り:
	•	csv_columns … 全体の列順（正規化後）
	•	norm_by_file … 各 CSV ファイルの「正規化後ヘッダ」リスト
	•	max_dup_count … {ベース名: 最大多重度}（ログ用）

ログ例
normalized series: {'qty': ['qty_1', 'qty_2', 'qty_3']}
header (items_01.csv) -> ['id','qty_1','qty_2','note']
union columns -> ['id','qty_1','qty_2','note','qty_3']

_make_temp_text_table(engine: Engine, columns: list[str]) -> str
	•	役割: pg_temp.<一意名> に TEXT 列だけの一時テーブルを作成し、完全修飾名（FQTN）を返す。
	•	意図: まず TEMP にロード→最後にまとめて UPSERT の方が安全・高速。

_copy_df_to_table(engine: Engine, df: pd.DataFrame, table_fqtn: str, columns: list[str]) -> None
	•	役割: DataFrame を COPY FROM STDIN で高速ロード。
	•	空欄→NULL: df.where(pd.notna(...), "") で空文字にして CSV 化 → COPY の NULL '' で 本物の NULL に。
	•	注意: Pylance が cursor() の with に警告を出す場合があるが、実行時は問題なし（psycopg2 は対応済）。気になる場合は cast で型ヒント補強。

upsert_table(engine, table_name, cfg, csv_root, chunksize=200_000, auto_add_columns=False) -> None
	•	役割: このモジュールの中心。CSV 群 → TEMP(TEXT) → UPSERT。
	•	処理:
	1.	対象 CSV 一覧取得 _iter_csv_files(...)
	2.	ヘッダ正規化 _union_csv_headers(...)（重複列は _1.._k）
	3.	ターゲットテーブル確認：
	•	無ければ TEXT + PK で作成（create_text_table）
	•	既存なら列取得し、不足列は auto_add_columns=True 時のみ ALTER TABLE ADD COLUMN
	4.	TEMP 作成 _make_temp_text_table(...)
	5.	CSV 読み込み（names=正規化後ヘッダ、mangle_dupe_cols=False）→ 欠け列は pd.NA を補完 → COPY で TEMP へ投入
	6.	ON CONFLICT (pk...) DO UPDATE SET ... でターゲットに UPSERT
	•	NULL/型: すべて TEXT として取り込み、空欄は NULL。型変換は dbt に委譲。

snapshot_table_to_parquet(engine: Engine, table_name: str, out_root: Path) -> None
	•	役割: Postgres から全件 SELECT → Parquet に保存（out_root/YYYYMMDD/table.parquet）。
	•	用途: DB 再構築時の復元シード、外部分析用途。

clean_old_csvs(csv_root: Path, archive_root: Path|None, retention_days: int, dry_run: bool=True) -> None
	•	役割: CSV の保存期間を超えたファイルを削除 or アーカイブへ移動。
	•	実装: mtime が閾値を下回るファイルを対象に、--dry-run なら表示のみ。
	•	運用: 「消したくない」場合はまずアーカイブ運用にして、一定期間後にアーカイブを掃除。

CLI コマンド（main()）
	•	csv_to_db.py ingest [--table <name>] [--chunksize N] [--auto-add-columns]
	•	全テーブル or 指定テーブルを取り込み＆UPSERT
	•	csv_to_db.py snapshot [--table <name>]
	•	Postgres → Parquet にスナップショット
	•	csv_to_db.py clean [--archive] [--dry-run]
	•	CSV を削除 or アーカイブに移動（RETENTION_DAYS 準拠）

⸻

3. 実運用の流れ（Runbook）

3.1 初期セットアップ
	1.	環境変数を設定（.env / .env.prod）
	2.	取り込み対象を定義（ingestion/config/tables.yml）
	3.	DB 接続テスト

python -c "from utils import get_engine; print(get_engine().connect().closed)"


	4.	スキーマ作成（冪等）

python -c "from utils import get_engine, ensure_schema; e=get_engine(); ensure_schema(e, 'raw'); print('ok')"



3.2 取り込み（CSV → Postgres UPSERT）
	•	全テーブル

python ingestion/csv_to_db.py ingest --auto-add-columns


	•	単一テーブルのみ

python ingestion/csv_to_db.py ingest --table ratings --auto-add-columns


	•	ログの見方（重複カラム正規化）

[items] normalized series: {'qty': ['qty_1', 'qty_2', 'qty_3']}
[items] header (items_02.csv) -> ['id', 'qty_1', 'note']
[items] union columns       -> ['id', 'qty_1', 'qty_2', 'note', 'qty_3']
[items] Upsert completed.



ポイント
	•	初回はテーブルを TEXT＋PK で作成。
	•	以降、新しい列が CSV 側に増えたら --auto-add-columns で DB に列追加。
	•	空欄は本物の NULL で入る（“文字列の null” ではない）。

3.3 スナップショット（Postgres → Parquet）

python ingestion/csv_to_db.py snapshot              # 全テーブル
python ingestion/csv_to_db.py snapshot --table movies
# 出力例： data/parquet/20250131/movies.parquet

復元の思想
	•	まず 最新の Parquet を Postgres にロード（dbt で “種” にしてもOK）。
	•	その後、直近 CSV を再適用（UPSERT）して追いつく。

3.4 CSV のクリーンアップ（容量対策）

# まずはドライランで対象確認
python ingestion/csv_to_db.py clean --dry-run

# アーカイブに移動
python ingestion/csv_to_db.py clean --archive

# 完全削除（強い運用）
python ingestion/csv_to_db.py clean

運用方針
	•	いきなり削除より アーカイブ移動のほうが安全（巻き戻しが容易）。
	•	RETENTION_DAYS は、上流の抽出ポリシー（例：直近2ヶ月の重複多め CSV）に合わせて設定。

3.5 スケジューリング
	•	cron（例：毎日 2:00 取り込み → 2:30 スナップショット → 3:00 クリーン）

0 2 * * *  /usr/bin/python /app/ingestion/csv_to_db.py ingest --auto-add-columns >> /var/log/ingest.log 2>&1
30 2 * * * /usr/bin/python /app/ingestion/csv_to_db.py snapshot >> /var/log/snapshot.log 2>&1
0 3 * * *  /usr/bin/python /app/ingestion/csv_to_db.py clean --archive >> /var/log/clean.log 2>&1


	•	supervisord / Airflow 等に組み込む場合も同様に CLI を呼び出すだけでOK。

⸻

4. 運用 Tips / トラブルシュート
	•	重複カラムの意味づけ
	•	raw では qty_1, qty_2, ... に分離して安全に保持。
	•	解釈（どれを採用／合算／優先） は dbt の stg モデルでビジネスルール化。
	•	取り込みスキップ
	•	対象フォルダに CSV が無ければスキップ（ログにパスを出力）。
	•	巨大 CSV
	•	--chunksize を調整（例：50_000）。
	•	COPY は高速だが、メモリと I/O に注意。
	•	エンコーディング/区切り記号の混在
	•	必要に応じて tables.yml に encoding / sep 等のオプションを持たせ、read_csv() に渡す拡張を検討。
	•	型の扱い
	•	raw は すべて TEXT。欠損は NULL。
	•	型変換・正規化・制約は dbt 側で実施（責務分離）。
	•	Pylance の赤線（型警告）
	•	read_csv のキーワード組み合わせ、cursor() の with などは 実行時は問題なし。
	•	気になる場合は cast か # type: ignore を選択（チームポリシーに合わせる）。
	•	権限
	•	CREATE SCHEMA / CREATE TABLE / ALTER TABLE / COPY に必要な権限を付与。
	•	本番では接続ロールを “限定権限” にする（raw スキーマだけ書込など）。

⸻

5. よくある質問（FAQ）
	•	Q: CSV 側に空欄があると “文字列の null” になりませんか？
A: なりません。空欄は CSV 出力時に ""（空文字）→ COPY の NULL '' 設定により DB の NULL で保存されます。
	•	Q: 複合キーの UPSERT はできますか？
A: できます。primary_key: ["colA","colB"] を指定 → ON CONFLICT (colA,colB) DO UPDATE。
	•	Q: CSV に新しい列が増えたら？
A: --auto-add-columns を付ければ TEXT 列で自動追加。付けない場合は無視（ログに WARNING）。
	•	Q: 同名カラムが複数あって内容が違う場合は？
A: 出現順で xxx_1, xxx_2, ... に自動正規化。単独のファイルでも _1 に寄せて横断一貫性を担保します。

⸻

6. 付録：最小実行例

# 1) .env を用意（PGHOST 等）
# 2) テーブル定義（ingestion/config/tables.yml）を書く
# 3) 取り込み（全て）
python ingestion/csv_to_db.py ingest --auto-add-columns

# 4) スナップショット
python ingestion/csv_to_db.py snapshot

# 5) CSV クリーンアップ（まず dry-run）
python ingestion/csv_to_db.py clean --dry-run


⸻

以上です。
この運用に dbt の stg / models を重ねることで、**「raw は壊さず受け止める」「型・整形は下流で綺麗にする」**という設計を貫けます。
他に「tables.yml に encoding/sep を持たせたい」「Parquet をパーティション出力したい」などの要望があれば、拡張案もすぐ書き起こします。



# 全体的なパイプライン設計の提案について


いいね、ローカル運用＋将来オンプレ移行前提で“DBと同時に生データを残す”ハイブリッドに最適化した最小だけど伸びやすい構成を提案します。まずはフォルダ構成→役割→PATHSと.env→Makefile雛形、の順で。

⸻

推奨構成（最小で始めてそのまま伸ばせる）

ingestion/
├─ fetchers/                 # 取得系（手動補助・playwright・APIラッパなど）
│  ├─ __init__.py
│  ├─ playwright_fetch.py
│  └─ manual_drop_watch.py   # 手動置きファイル検知(オプション)
├─ pipelines/                # パイプライン実行スクリプト（CLI）
│  ├─ csv_to_db.py           # 既存：UPSERT/TEMP/COPY/Parquet/clean を担当
│  └─ validate.py            # 取得直後の軽い検証（行数/ヘッダ等）
├─ config/
│  ├─ tables.yml             # テーブル定義（folder, pk, encoding, 既定値など）
│  └─ logging.ini            # 将来 logging 切替用（最初は空でもOK）
├─ logs/                     # 実行ログ（Makefileからリダイレクト）
└─ bin/                      # 補助スクリプト/ワンライナー
   └─ stamp.sh

取得データや出力物は コード外（リポジトリ外でも可） の data/ 配下にまとめます（下記）。

data/
├─ landing/                  # 生データの永続保管（immutable）
│  └─ team=<team>/table=<table>/
│     └─ run_date=YYYYMMDD/
│        └─ batch_id=<uuid>/
│           ├─ parts/       # 元CSV(複数可)
│           │  ├─ <filename1>.csv
│           │  └─ <filename2>.csv
│           └─ manifest.json  # 期間/件数/ハッシュ/取得方法など
├─ work/                     # 作業域（必要なら）
│  └─ tmp/                   # 一時展開など（再生成可）
├─ db_ingestion/             # Postgres取り込み対象“ビュー”（結合しやすい置き場）
│  └─ team1/
│     ├─ table1/ *.csv
│     └─ table2/ *.csv
├─ archive/                  # 取り込み済みCSVの退避（一定期間で削除）
└─ parquet/                  # Postgres→Parquet スナップショット
   └─ YYYYMMDD/
      ├─ table1.parquet
      └─ table2.parquet

	•	landing：取得した“そのまま”を必ず残す（再現性・監査用）。
	•	db_ingestion：いまの csv_to_db.py が読むルート。landing から必要に応じて整形/結合して配置（最初は手で置いてOK、後で自動化）。
	•	archive：取り込み後の退避先（保持期間を過ぎたら削除）。
	•	parquet：日次スナップショット（DB復元のシード）。

「landingはimmutable、db_ingestionは取り込み用の見せ方」と分けると、将来的に処理を差し替えても影響範囲が明確になります。

⸻

PATHS と .env（既定値）

utils.get_paths() はこの既定を持たせておくのが楽です：

DATA_DIR=./data
CSV_ROOT=${DATA_DIR}/db_ingestion
PARQUET_ROOT=${DATA_DIR}/parquet
ARCHIVE_ROOT=${DATA_DIR}/archive
LANDING_ROOT=${DATA_DIR}/landing

	•	既存の CSV_ROOT だけ db_ingestion に向け直す（あなたの新しい配置に合致）。
	•	LANDING_ROOT を新設（fetchers がここへ保存）。
	•	取り込み対象は tables.yml の folder: team1/table1 のように階層付きで指定。

tables.yml（例：エンコーディングのデフォルト＋個別上書き）：

defaults:
  encoding: utf-8
  chunksize: 200000
  filename_glob: "*.csv"

tables:
  table1:
    folder: team1/table1
    primary_key: ["id"]
    # encoding は defaults を使用

  table2:
    folder: team1/table2
    primary_key: ["k1","k2"]
    encoding: shift_jis


⸻

使い方（Makefile ドリブン）

最初は Airflow なしで Makefile で一気通貫を回します。

Makefile（雛形）

# ====== 基本設定 ======
PYTHON := python
ING := ingestion/pipelines/csv_to_db.py
VAL := ingestion/pipelines/validate.py

# env（必要に応じて .env を export する）
include .env
export

# ====== タスク ======

## 1) 取得（手動/自動）
# 手動: ダウンロードしたCSVを data/landing/.../parts に置く
# 自動: playwright で取得 → landing に保存（別スクリプト/将来）
fetch:
	@echo "[fetch] put CSVs under data/landing/... and/or run fetchers/*"

## 2) 検証（任意：行数/ヘッダ/期間など）
validate:
	$(PYTHON) $(VAL) --landing $(LANDING_ROOT)

## 3) 取り込み（UPSERT）
# 全テーブル
ingest:
	$(PYTHON) $(ING) ingest --auto-add-columns | tee -a ingestion/logs/ingest.log

# 個別テーブル
ingest-%:
	$(PYTHON) $(ING) ingest --table $* --auto-add-columns | tee -a ingestion/logs/ingest_$*.log

## 4) スナップショット（Postgres -> Parquet）
snapshot:
	$(PYTHON) $(ING) snapshot | tee -a ingestion/logs/snapshot.log

snapshot-%:
	$(PYTHON) $(ING) snapshot --table $* | tee -a ingestion/logs/snapshot_$*.log

## 5) クリーンアップ（取り込み済みCSVを退避/削除）
# まずは dry-run で確認 → 問題なければ archive に移動
clean-dry:
	$(PYTHON) $(ING) clean --dry-run | tee -a ingestion/logs/clean.log

clean-archive:
	$(PYTHON) $(ING) clean --archive | tee -a ingestion/logs/clean.log

clean-hard:
	$(PYTHON) $(ING) clean | tee -a ingestion/logs/clean.log

## 6) ワンショット（取得→検証→取り込み→スナップショット）
all: fetch validate ingest snapshot
	@echo "DONE"

make ingest-table1 のように テーブル個別ターゲットを使えるのが運用で便利。

⸻

運用フロー（最初は手動＋一部自動）
	1.	（手動 or playwright）で取得
	•	保存先：data/landing/team=<team>/table=<table>/run_date=YYYYMMDD/batch_id=<uuid>/parts/*.csv
	•	manifest.json（期間・件数・ハッシュ）を簡易でも良いので作ると後々助かります。
	2.	必要なら整形して db_ingestion に配置
	•	当面は“landing → 必要ファイルを db_ingestion にコピー”でもOK。
	•	後で整形/結合（例えば1ファイルにまとめる）をスクリプト化しても良いです。
	3.	取り込み（UPSERT）
	•	make ingest（全テーブル）または make ingest-table1（個別）。
	4.	スナップショット
	•	make snapshot → data/parquet/YYYYMMDD/*.parquet に出力（DB復元用シード）。
	5.	クリーンアップ
	•	make clean-dry で対象確認 → make clean-archive で退避、一定期間後に clean-hard。

将来 Airflow に移すときは、Make タスク1つ＝DAGの1タスク相当として移植できます。

⸻

これで何が嬉しいか（要点）
	•	いまの csv_to_db.py を“そのまま”活かせる（CSV_ROOT を db_ingestion に変更するだけ）。
	•	生データは landing に必ず残すので、事故っても後戻りできる。
	•	Makefile でパイプライン化しておけば、将来 Airflow に移行する時も分解が簡単。
	•	フォルダ責務が明確（landing=原本、db_ingestion=取り込みビュー、parquet=DBスナップショット、archive=退避）。

⸻

必要なら：
	•	manifest.json のサンプルスキーマ
	•	validate.py の最小実装（件数/ヘッダ/重複Keyチェック）
	•	utils.get_paths() の pathlib 版（LANDING_ROOT 追加）

もすぐ出します。どこから作っていきましょう？