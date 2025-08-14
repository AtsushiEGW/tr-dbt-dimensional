
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