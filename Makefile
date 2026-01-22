# Makefile for Python data generation scripts and Docker stack management

# Configurations

PYTHON := python
SCRIPTS_DIR := src/data_generation_scripts

# Docker services groups
STREAMING_SERVICES := zookeeper kafka kafdrop marketing_dw_postgres clickhouse jobmanager taskmanager debezium schema-registry \
                      postgres_airflow airflow-init airflow-webserver airflow-scheduler

BATCH_SERVICES := clickhouse postgres_airflow airflow-init airflow-webserver airflow-scheduler dbt pyspark

DASHBOARD_SERVICES := clickhouse superset superset_worker superset_beat redis postgres-superset


# Targets
.PHONY: all generate_all generate_master_customer_ids generate_batch_customers_profile_data \
        generate_streaming_purchases generate_web_events streaming_stack down_streaming_stack \
        batch_stack batch_stack_down dashboard_stack down_dashboard_stack

# Python data generation scripts
generate_master_customer_ids:
	@echo "Running generate_master_customer_ids.py..."
	$(PYTHON) $(SCRIPTS_DIR)/generate_master_customer_ids.py

generate_batch_customers_profile_data:
	@echo "Running generate_batch_customers_profile_data.py..."
	$(PYTHON) $(SCRIPTS_DIR)/generate_batch_customers_profile_data.py

generate_streaming_purchases:
	@echo "Running generate_streaming_purchases.py..."
	$(PYTHON) $(SCRIPTS_DIR)/generate_streaming_purchases.py

generate_web_events:
	@echo "Running generate_web_events.py..."
	$(PYTHON) $(SCRIPTS_DIR)/generate_web_events.py

# Run all data generation scripts
generate_all: generate_master_customer_ids generate_batch_customers_profile_data generate_streaming_purchases generate_web_events

# Docker stack management
# Start streaming stack
streaming_stack:
	docker compose up -d $(STREAMING_SERVICES)

# Stop streaming stack
down_streaming_stack:
	docker compose stop $(STREAMING_SERVICES)

# Start batch/orchestration stack
batch_stack:
	docker compose up -d $(BATCH_SERVICES)

# Stop batch/orchestration stack
batch_stack_down:
	docker compose stop $(BATCH_SERVICES)

# Start dashboard/visualization stack
dashboard_stack:
	docker compose up -d $(DASHBOARD_SERVICES)

# Stop dashboard/visualization stack
down_dashboard_stack:
	docker compose stop $(DASHBOARD_SERVICES)
