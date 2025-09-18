# Makefile for submitting PyFlink jobs via Docker
# and running Python scripts for data generation

# Container name
FLINK_JOBMANAGER=jobmanager

# Job paths inside container
PURCHASE_AGG=/opt/src/jobs/purchase_agg.py
PURCHASE_EVENTS=/opt/src/jobs/purchase_events_job.py
WEBSITE_AGG=/opt/src/jobs/website_agg.py
WEBSITE_EVENTS=/opt/src/jobs/website_events_job.py

# Local Python scripts for generating data
PYTHON = python
SCRIPTS_DIR = src/data_generation_scripts
STREAMING_PURCHASES=$(SCRIPTS_DIR)/generate_streaming_purchases.py
WEB_EVENTS=$(SCRIPTS_DIR)/generate_web_events.py

# -----------------------------
# Targets
# -----------------------------
.PHONY: all purchase_agg purchase_events website_agg website_events \
        generate_streaming_purchases generate_web_events generate_all \
        streaming_stack airflow_stack down_streaming_stack down_airflow_stack

all: purchase_agg purchase_events website_agg website_events

# -----------------------------
# Start only streaming stack (Kafka + Flink + Postgres)
# -----------------------------
streaming_stack:
	docker compose up -d zookeeper kafka kafdrop marketing_dw_postgres clickhouse kafdrop jobmanager taskmanager debezium schema-registry

down_streaming_stack:
	docker compose stop zookeeper kafka kafdrop marketing_dw_postgres clickhouse kafdrop jobmanager taskmanager debezium schema-registry

# -----------------------------
# Start orchestration + visualization stack (Airflow, DBT, Spark, Superset)
# -----------------------------
airflow_stack:
	docker compose up -d  clickhouse marketing_dw_postgres postgres_airflow airflow-init airflow-webserver airflow-scheduler \
		dbt pyspark debezium schema-registry

down_airflow_stack:
	docker compose stop clickhouse marketing_dw_postgres postgres_airflow airflow-init airflow-webserver airflow-scheduler \
		dbt pyspark debezium schema-registry

dashboard_stack:
	docker compose up -d clickhouse superset superset_worker superset_beat redis postgres-superset

down_dashboard_stack:
	docker compose stop clickhouse superset superset_worker superset_beat redis postgres-superset

# -----------------------------
# PyFlink jobs
# -----------------------------
purchase_agg:
	docker exec $(FLINK_JOBMANAGER) ./bin/flink run -d -py $(PURCHASE_AGG)

purchase_events:
	docker exec $(FLINK_JOBMANAGER) ./bin/flink run -d -py $(PURCHASE_EVENTS)

website_agg:
	docker exec $(FLINK_JOBMANAGER) ./bin/flink run -d -py $(WEBSITE_AGG)

website_events:
	docker exec $(FLINK_JOBMANAGER) ./bin/flink run -d -py $(WEBSITE_EVENTS)

# -----------------------------
# Python data generation scripts
# -----------------------------
generate_streaming_purchases:
	@echo "Running generate_streaming_purchases.py..."
	$(PYTHON) $(STREAMING_PURCHASES)

generate_web_events:
	@echo "Running generate_web_events.py..."
	$(PYTHON) $(WEB_EVENTS)

# Optional: run both generation scripts
generate_all:
	$(MAKE) generate_streaming_purchases
	$(MAKE) generate_web_events
