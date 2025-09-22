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