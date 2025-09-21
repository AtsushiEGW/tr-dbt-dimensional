
COMMON_OPTS = --mode upsert --create

OPTS_spotify := --pk


.PHONY: ingestion-sales
ingestion-sales:
	python ingestion/csv_to_db.py \
	--host db \
	--data-root raw_data/ \
	--table sales_order_detail \
	--schema test_ingest \
	--create \
	--truncate \
	--mode upsert \
	--pk salesorderdetailid



.PHONY: ingestion-sales
ingestion-sales:
	python ingestion/csv_to_db.py \
	--host db \
	--data-root raw_data/ \
	--table sales_order_detail \
	--schema test_ingest \
	--create \
	--truncate \
	--mode upsert \
	--pk salesorderdetailid

describe:
	echo "===Project Directory Structure===\n" > file_describe.txt
	tree -a -I "file_describe.txt|Makefile|.git|.gitignore|.vscode|adventureworks|data|raw_data|docs|memo.md|note|supervisord.pid|test.py|backup" >> file_describe.txt

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


	echo "\n\n\n=== .env ===\n" >> file_describe_all_files.txt
	echo "===Project Directory Structure===\n" > file_describe_all_files.txt
	tree -a -I ".git|.gitignore|.vscode|adventureworks|memo.md|note|supervisord.pid|test.py|backup" >> file_describe_all_files.txt