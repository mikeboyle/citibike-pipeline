import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from citibike.config import load_env_config
from citibike.ingestion.borough_boundaries import ingest_borough_boundaries
from citibike.dbt import run_dbt_command

# Default arguments for all tasks in this DAG
default_args = {
    'owner': 'citibike-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,  # Set to True and add email for notifications
    'email_on_retry': False,
    'retries': 0,  # don't retry failed task
}

# Define the DAG
dag = DAG(
    'boundaries_pipeline',
    default_args=default_args,
    description='Get NYC borough boundaries geojson from API and persist to BigQuery',
    schedule_interval=None,  # Manual trigger only (no automatic scheduling)
    catchup=False,  # Don't run for past dates
    tags=['citibike'],
)

def ingest_boundaries_task():
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)
    
    ingest_borough_boundaries()

def transform_boundaries_task():
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)
    
    dbt_command = "dbt run --select +silver_nyc_borough_boundaries"
    run_dbt_command(dbt_command.split())

# Define tasks
task_ingest_borough_boundaries = PythonOperator(
    task_id='ingest_borough_boundaries',
    python_callable=ingest_boundaries_task,
    dag=dag,
)

task_transform_borough_boundaries = PythonOperator(
    task_id='silver_borough_boundaries',
    python_callable=transform_boundaries_task,
    dag=dag,
)

task_ingest_borough_boundaries >> task_transform_borough_boundaries # pyright: ignore[reportUnusedExpression]
