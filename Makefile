
describe-docker:
	echo "===Project Directory Structure===\n" > file_describe.txt
	tree -a -I "file_describe.txt|Makefile|.git|.gitignore|.vscode|adventureworks|data|raw_data|data_source|docs|memo.md|note|supervisord.pid|test.py|backup" >> file_describe.txt

	echo "#####################################################################################" >> file_describe.txt
	echo "\n\n\n=== .devcontainer/devcontainer.json===\n" >> file_describe.txt
	cat .devcontainer/devcontainer.json >> file_describe.txt

	echo "\n\n\n=== .devcontainer/Dockerfile===\n" >> file_describe.txt
	cat .devcontainer/Dockerfile >> file_describe.txt

	echo "\n\n\n=== .devcontainer/supervisord.conf===\n" >> file_describe.txt
	cat .devcontainer/supervisord.conf >> file_describe.txt

	echo "\n\n\n=== docker-compose.yml===\n" >> file_describe.txt
	cat docker-compose.yml >> file_describe.txt

	echo "\n\n\n=== docker-compose.dev.yml===\n" >> file_describe.txt
	cat docker-compose.dev.yml >> file_describe.txt

	echo "\n\n\n=== requirements.txt===\n" >> file_describe.txt
	cat requirements.txt >> file_describe.txt

	echo "\n\n\n=== .dockerignore ===\n" >> file_describe.txt
	cat .dockerignore >> file_describe.txt

	echo "\n\n\n=== .env ===\n" >> file_describe.txt
	cat .env >> file_describe.txt

	echo "\n\n\n=== superset/Dockerfile.superset ===\n" >> file_describe.txt
	cat superset/Dockerfile.superset >> file_describe.txt

	echo "\n\n\n=== superset/superset_config.py ===\n" >> file_describe.txt
	cat superset/superset_config.py >> file_describe.txt

	echo "\n\n\n=== .env ===\n" >> file_describe_all_files.txt
	echo "===Project Directory Structure===\n" > file_describe_all_files.txt
	tree -a -I ".git|.gitignore|.vscode|adventureworks|memo.md|note|supervisord.pid|test.py|backup" >> file_describe_all_files.txt


describe-ingestion:
	echo "===Project Directory Structure===\n" > file_describe.txt
	tree -a -I "file_describe.txt|Makefile|.git|.gitignore|.vscode|docs|memo.md|note|supervisord.pid|test.py|backup|" >> file_describe.txt

	echo "\n\n\n=== ingestion/config/tables.yml ===\n" >> file_describe.txt
	cat ingestion/config/tables.yml >> file_describe.txt

	echo "\n\n\n=== ingestion/utils.py ===\n" >> file_describe.txt
	cat ingestion/utils.py >> file_describe.txt

	echo "\n\n\n=== ingestion/pipelines/clean_landing.py ===\n" >> file_describe.txt
	cat ingestion/pipelines/clean_landing.py >> file_describe.txt

	echo "\n\n\n=== ingestion/pipelines/csv_to_db.py ===\n" >> file_describe.txt
	cat ingestion/pipelines/csv_to_db.py >> file_describe.txt

	echo "\n\n\n=== ingestion/pipelines/land_import.py ===\n" >> file_describe.txt
	cat ingestion/pipelines/land_import.py >> file_describe.txt

	echo "\n\n\n=== ingestion/pipelines/promote.py ===\n" >> file_describe.txt
	cat ingestion/pipelines/promote.py >> file_describe.txt

	echo "\n\n\n=== ingestion/pipelines/validate.py ===\n" >> file_describe.txt
	cat ingestion/pipelines/validate.py >> file_describe.txt

	echo "\n\n\n=== .env ===\n" >> file_describe.txt
	cat .env >> file_describe.txt



tree:
	echo "\n\n\n=== .env ===\n" >> file_describe_all_files.txt
	echo "===Project Directory Structure===\n" > file_describe_all_files.txt
	tree -a -I ".git|.gitignore|.vscode|adventureworks|memo.md|note|supervisord.pid|test.py|backup|docs" >> file_describe_all_files.txt

####################

# ===========================
# Makefile (module execution / A案)
# ===========================
PYTHON := python

# .env を読み込む（存在すれば）
ifneq (,$(wildcard .env))
include .env
export
endif

# ログディレクトリ
LOGDIR := ingestion/logs

# すべてのターゲットでログDirを用意
$(LOGDIR):
	mkdir -p $(LOGDIR)

# -------------------------------------------------
# 1) 手動CSVを landing へ取り込み（manifest生成, latest更新）
# 例:
#   make land-import-movies SRC=data/manual_drop/namespace=ingest_test/table=movies NAMESPACE=ingest_test MOVE=1
# 必須: SRC, NAMESPACE
# 任意: RUN_DATE (YYYYMMDD), MOVE(=1 なら移動), patternは land_import.py の既定 *.csv を使用
# -------------------------------------------------
.PHONY: land-import-%
land-import-%: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.land_import \
		--src "$(SRC)" \
		--namespace "$(NAMESPACE)" \
		--table "$*" \
		$(if $(RUN_DATE),--run-date $(RUN_DATE),) \
		$(if $(MOVE),--move,) \
		--latest | tee -a $(LOGDIR)/land_import_$*.log

# -------------------------------------------------
# 2) landing の簡易検証
# .env の LANDING_ROOT を使う場合は値付き、未指定ならオプション自体を付けない
# -------------------------------------------------
.PHONY: validate
validate: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.validate \
		$(if $(LANDING_ROOT),--landing $(LANDING_ROOT),) | tee -a $(LOGDIR)/validate.log

# -------------------------------------------------
# 3) landing → db_ingestion へのプロモート
# 例:
#   make promote-movies NAMESPACE=ingest_test DATE=20251004
#   make promote-movies NAMESPACE=ingest_test DATE=20251004 BATCH=20251004T010203Z_ab12cd34
# 必須: NAMESPACE, DATE(YYYYMMDD)
# 任意: BATCH（未指定なら latest）
# -------------------------------------------------
.PHONY: promote-%
promote-%: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.promote \
		--namespace "$(NAMESPACE)" \
		--table "$*" \
		--run-date "$(DATE)" \
		$(if $(BATCH),--batch-id $(BATCH),--batch-id latest) | tee -a $(LOGDIR)/promote_$*.log

# -------------------------------------------------
# 4) 取り込み（UPSERT）
# 全テーブル or 単一テーブル
# -------------------------------------------------
.PHONY: ingest
ingest: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db ingest --auto-add-columns | tee -a $(LOGDIR)/ingest.log

.PHONY: ingest-%
ingest-%: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db ingest --table "$*" --auto-add-columns | tee -a $(LOGDIR)/ingest_$*.log

# -------------------------------------------------
# 5) スナップショット（DB → Parquet）
# -------------------------------------------------
.PHONY: snapshot
snapshot: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db snapshot | tee -a $(LOGDIR)/snapshot.log

.PHONY: snapshot-%
snapshot-%: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db snapshot --table "$*" | tee -a $(LOGDIR)/snapshot_$*.log

# -------------------------------------------------
# 6) クリーンアップ（db_ingestion 配下の古い CSV）
#   RETENTION_DAYS は .env で設定（既定 60 はスクリプト側）
# -------------------------------------------------
.PHONY: clean-dry
clean-dry: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db clean --dry-run | tee -a $(LOGDIR)/clean.log

.PHONY: clean-archive
clean-archive: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db clean --archive | tee -a $(LOGDIR)/clean.log

.PHONY: clean-hard
clean-hard: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.csv_to_db clean | tee -a $(LOGDIR)/clean.log

# -------------------------------------------------
# 7) landing の圧縮/削除 運用
#   LANDING_RETENTION_DAYS / LANDING_COMPRESS_AFTER_DAYS /
#   LANDING_KEEP_PER_NAMESPACE は .env で指定
# -------------------------------------------------
.PHONY: clean-landing
clean-landing: | $(LOGDIR)
	$(PYTHON) -m ingestion.pipelines.clean_landing | tee -a $(LOGDIR)/clean_landing.log

# -------------------------------------------------
# ワンショット（検証 → 取り込み → スナップショット）
# -------------------------------------------------
.PHONY: all
all: validate ingest snapshot

# ===== Replay (landing -> db_ingestion -> upsert) =====
REPLAY := ingestion/pipelines/replay.py

# 全ての namespace/table を landing から再生（時系列コピー→UPSERT）。最後に snapshot も出す
replay-all:
	$(PYTHON) -m ingestion.pipelines.replay --snapshot | tee -a ingestion/logs/replay_all.log

# 指定 namespace / table を再生（最新まで）。--since を付けるとその日付以降のみ
# 例: make replay-table NAMESPACE=ingest_test TABLE=movies
# 例: make replay-table NAMESPACE=ingest_test TABLE=movies SINCE=20240901
replay-table:
	$(PYTHON) -m ingestion.pipelines.replay \
		$(if $(NAMESPACE),--namespace "$(NAMESPACE)",) \
		$(if $(TABLE),--table "$(TABLE)",) \
		$(if $(SINCE),--since "$(SINCE)",) \
		--snapshot | tee -a ingestion/logs/replay_$(TABLE).log



csv_demo_to_manual_drop:
	cp data/csvs_demo/links/* data/manual_drop/namespace=ingest_test/table=links
	cp data/csvs_demo/movies/* data/manual_drop/namespace=ingest_test/table=movies
	cp data/csvs_demo/ratings/* data/manual_drop/namespace=ingest_test/table=ratings
	cp data/csvs_demo/tags/* data/manual_drop/namespace=ingest_test/table=tags

###################
### docker 関連
###################
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml
SPS := /app/.venv/bin/superset

.PHONY: docker-superset-init
docker-superset-init:
	@$(COMPOSE) up -d db redis
	@$(COMPOSE) exec db bash -lc " \
	  until pg_isready -U \"$$POSTGRES_USER\" -h 127.0.0.1 -p $${POSTGRES_PORT:-5432}; do \
	    echo 'waiting for postgres...'; sleep 1; \
	  done; \
	  psql -U \"$$POSTGRES_USER\" -h 127.0.0.1 -p $${POSTGRES_PORT:-5432} -d postgres -tAc \"SELECT 1 FROM pg_database WHERE datname='superset'\" \
	    | grep -q 1 \
	    || psql -U \"$$POSTGRES_USER\" -h 127.0.0.1 -p $${POSTGRES_PORT:-5432} -d postgres -c \"CREATE DATABASE superset;\" \
	"
	@$(COMPOSE) up -d superset
	@$(COMPOSE) exec superset bash -lc "$(SPS) db upgrade"
	@$(COMPOSE) exec superset bash -lc " \
	  $(SPS) fab create-admin \
	    --username \"$$SUPERSET_ADMIN_USERNAME\" \
	    --firstname \"$$SUPERSET_ADMIN_FIRST_NAME\" \
	    --lastname  \"$$SUPERSET_ADMIN_LAST_NAME\" \
	    --email     \"$$SUPERSET_ADMIN_EMAIL\" \
	    --password  \"$$SUPERSET_ADMIN_PASSWORD\" || true \
	"
	@$(COMPOSE) exec superset bash -lc "$(SPS) init"


docker-start:
	docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml up -d db redis superset-worker superset-beat