from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import timedelta, datetime
import os
import logging
import subprocess
from airflow.utils.trigger_rule import TriggerRule

# -------------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CustomerEventsDAG")

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['ziadashraf98765@gmail.com']
}

with DAG(
    "customer_events_pipeline",
    default_args=default_args,
    description="Batch + Streaming + Join pipeline with Spark, Iceberg, Postgres, dbt",
    # every 3 hours
    schedule_interval="0 */6 * * *",  # every 6 hours
    start_date=datetime(2025, 9, 8),
    max_active_runs=1,   # ⬅️ ensures only ONE run is active at a time
    concurrency=1,       # ⬅️ ensures only ONE task across the DAG runs at once
    catchup=True,
    tags=['batch' ,'customer', 'events', 'pipeline', 'clickhouse' , 'spark', 'dbt', 'iceberg']
) as dag:


    dbt_run_stream_staging = BashOperator(
        task_id="dbt_run_stream_staging",
        bash_command='docker exec dbt bash -c "cd /dbt && dbt run --models stg_purchase_events stg_website_events"',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_stream_intermediate = BashOperator(
        task_id="dbt_run_stream_intermediate",
        bash_command='docker exec dbt bash -c "cd /dbt && dbt run --models int_customer_purchases int_customer_web_activity"',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_funnel_mart = BashOperator(
        task_id="dbt_run_funnel_mart",
        bash_command='docker exec dbt bash -c "cd /dbt && dbt run --models marketing_funnel"',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_ml_mart = BashOperator(
        task_id="dbt_run_ml_mart",
        bash_command='docker exec dbt bash -c "cd /dbt && dbt run --models int_customer_profiles_ml"',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_country_mart = BashOperator(
        task_id="dbt_run_country_mart",
        bash_command='docker exec dbt bash -c "cd /dbt && dbt run --models customer_country_summary"',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_customer_monthly_mart = BashOperator(
        task_id="dbt_run_customer_monthly_mart",
        bash_command='docker exec dbt bash -c "cd /dbt && dbt run --models  customer_monthly_summary"',
        trigger_rule=TriggerRule.ALL_DONE,
    )


    create_s3_int_customer_profiles_ml = BashOperator(
    task_id="create_s3_int_customer_profiles_ml",
    bash_command=(
     """docker exec clickhouse clickhouse-client --query "CREATE OR REPLACE TABLE s3_int_customer_profiles_ml ENGINE = S3('http://minio:9000/marketing-data-lake/raw_ml_table/int_customer_profiles_ml', 'minioadmin', 'minioadmin', 'Parquet') AS SELECT * FROM marts.int_customer_profiles_ml SETTINGS s3_truncate_on_insert = 1;"  """  
    ),
    trigger_rule=TriggerRule.ALL_DONE,
    )

    spark_iceberg_ml_table = BashOperator(
        task_id="spark_iceberg_ml_table",
        bash_command="docker exec pyspark python /opt/spark/jobs/load_ml_table_to_iceberg_minio.py",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -------------------
    # DAG Dependencies
    dbt_run_stream_staging >> dbt_run_stream_intermediate
    dbt_run_stream_intermediate >> dbt_run_funnel_mart
    dbt_run_funnel_mart >> dbt_run_ml_mart >> create_s3_int_customer_profiles_ml >> spark_iceberg_ml_table
    dbt_run_ml_mart >> dbt_run_customer_monthly_mart
    dbt_run_ml_mart >> dbt_run_country_mart
