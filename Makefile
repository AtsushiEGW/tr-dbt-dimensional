
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


file-describe:
	echo "現在のプロジェクトの主なフォルダ構成とファイルの中身は以下となっています" > file-describe.txt
	echo "\n====== folder 構造 =====" >> file-describe.txt
	echo "- project_dir/" >> file-describe.txt
	echo "    - .devcontainer/" >> file-describe.txt
	echo "          - devcontainer.json" >> file-describe.txt
	echo "          - Dockerfile" >> file-describe.txt
	echo "          - supervisord.conf" >> file-describe.txt
	echo "    - my_project/" >> file-describe.txt
	echo "    - docker-compose.yml" >> file-describe.txt
	echo "    - .env" >> file-describe.txt
	echo "    - requirements.txt" >> file-describe.txt
	echo "\n====== .devcontainer/devcontainer.json ======" >> file-describe.txt
	cat .devcontainer/devcontainer.json >> file-describe.txt
	echo "\n====== .devcontainer/Dockerfile ======" >> file-describe.txt
	cat .devcontainer/Dockerfile >> file-describe.txt
	echo "\n====== .devcontainer/supervisord.conf ======" >> file-describe.txt
	cat .devcontainer/supervisord.conf >> file-describe.txt
	echo "\n====== docker-compsoe.ym ======" >> file-describe.txt
	cat docker-compose.yml >> file-describe.txt
	echo "\n====== requirements.txt ======" >> file-describe.txt
	cat requirements.txt >> file-describe.txt