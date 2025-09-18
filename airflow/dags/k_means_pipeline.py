from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# -------------------------------
# DAG default arguments
# -------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['ziadashraf98765@gmail.com']
}

# -------------------------------
# DAG definition
# -------------------------------
dag = DAG(
    'daily_customer_clustering_bash',
    default_args=default_args,
    description='Daily orchestration of Spark clustering job using Bash',
    schedule='0 18 * * *',  # Every day at 18:00
    start_date=datetime(2025, 8, 26),
    catchup=True,
    tags=['customer groups', 'clustering', 'k-means', 'spark', 'bash' , 'iceberg']
)

# -------------------------------
# BashOperator to trigger Spark job
# -------------------------------
run_spark_job = BashOperator(
    task_id='run_customer_clustering',
    bash_command="docker exec pyspark python /opt/spark/jobs/ml_clustering.py",
    dag=dag
)


monitor_customer_clustering = BashOperator(
task_id = 'monitor_customer_clustering',
bash_command="docker exec pyspark python /opt/spark/jobs/monitor_customer_clustering.py" ,
dag = dag)


cluster_analysis_table = BashOperator(
task_id = 'cluster_analysis_table',
bash_command="docker exec pyspark python /opt/spark/jobs/cluster_analysis_table.py" ,
dag = dag)

run_spark_job >> monitor_customer_clustering >> cluster_analysis_table