from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    "streaming_flink_agg_pipeline",
    default_args=default_args,
    description="Start streaming stack, run Flink jobs, stop stack after fixed time",
    schedule_interval="0 */3 * * *",
    start_date=datetime(2025, 8, 26),
    catchup=True,
    tags=['streaming' , 'customer', 'events', 'pipeline', 'spark', 'dbt', 'iceberg']
) as dag:


    # Run Flink jobs with 60-second timeout (always success)
    run_website_events_agg = BashOperator(
        task_id="run_website_events",
        bash_command=(
            "docker exec jobmanager ./bin/flink run -d -py /opt/src/jobs/website_agg.py --timeout 600"
        ),
        do_xcom_push=False
    )

    run_purchase_agg = BashOperator(
        task_id="run_purchase_agg",
        bash_command=(
            "docker exec jobmanager ./bin/flink run -d -py /opt/src/jobs/purchase_agg.py --timeout 600"
        ),
        do_xcom_push=False
    )



[run_website_events_agg, run_purchase_agg]
