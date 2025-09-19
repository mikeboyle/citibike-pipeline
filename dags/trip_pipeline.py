import json
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from citibike.config import load_env_config
from citibike.ingestion.trips import ingest_trip_data
from citibike.ingestion.stations import ingest_station_data
from citibike.dbt import run_dbt_command
from citibike.utils.date_helpers import now_nyc_datetime

def extract_year_month(**context):
    """Extract year and month from DAG config"""
    params = context.get("params", {})

    # Validate year range (CitiBike started in 2013)
    year = int(params.get("year", 0))
    current_year = datetime.now().year
    if not (2015 <= year <= current_year):
        raise ValueError(f"Year must be between 2013 and {current_year}, got {year}")

    month = int(params.get("month", 0))
    # Validate month range
    if not (1 <= month <= 12):
        raise ValueError(f"Month must be between 1 and 12, got {month}")
    
    # Additional validation: month must be < current month
    if year == current_year:
        current_month = datetime.now().month
        if month >= current_month:
            raise ValueError(f"Cannot process current or future months. Current month is {current_month}, requested {month}")

    # Validation complete! Create month_key
    month_key = f"{year:04d}-{month:02d}"

    # Store in XCom for other tasks to use
    context["task_instance"].xcom_push(key="year", value=year)
    context["task_instance"].xcom_push(key="month", value=month)
    context["task_instance"].xcom_push(key="month_key", value=month_key)

def run_ingest_station_data():
    # Load environment variables from config
    env_name = os.environ.get("CITIBIKE_ENV", "dev")
    load_env_config(env_name)

    print(f"Ingesting station data")
    ingestion_date = now_nyc_datetime()
    ingest_station_data(ingestion_date)

def run_ingest_trip_data(**context):
    """Task to ingest trips data"""
    # Load environment variables from config
    env_name = os.environ.get("CITIBIKE_ENV", "dev")
    load_env_config(env_name)

    # Get dag job args for year and month
    year = context["task_instance"].xcom_pull(key="year")
    month = context["task_instance"].xcom_pull(key="month")
    month_key = context["task_instance"].xcom_pull(key="month_key")

    print(f"Ingesting trips data for {month_key}")
    ingest_trip_data(year, month)

def run_transform_data(**context):
    """Task to run dbt transformations through silver and gold layers"""
    # Load environment variables from config
    env_name = os.environ.get("CITIBIKE_ENV", "dev")
    load_env_config(env_name)

    # Get dag job args for month_key
    month_key = context["task_instance"].xcom_pull(key="month_key")

    print(f"Transforming data for {month_key}")

    dbt_vars = json.dumps({ "month_key": month_key })
    dbt_command = ["dbt", "run", "--selector", "dashboard_models", "--vars", dbt_vars]
    run_dbt_command(dbt_command)

# Define the DAG

# Default arguments for all tasks in this DAG
default_args = {
    "owner": "citibike-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,  # Set to True and add email for notifications
    "email_on_retry": False,
    "retries": 0,  # don't retry failed task
}

dag = DAG(
    "trip_pipeline",
    default_args=default_args,
    description="Ingest latest raw stations and trip data, then transform to silver stations, silver trips, and gold dashboard and dimensional models",
    schedule_interval=None, # Manual trigger for now
    catchup=False, # Don't run for past dates
    tags=["citibike"],
    params={
        "year": 0,
        "month": 0,
    }
)

# Define tasks
extract_params_task = PythonOperator(
    task_id="extract_parameters",
    python_callable=extract_year_month,
    dag=dag,
)

ingest_stations_task = PythonOperator(
    task_id="ingest_stations",
    python_callable=run_ingest_station_data,
    dag=dag,
)

ingest_trips_task = PythonOperator(
    task_id="ingest_trips",
    python_callable=run_ingest_trip_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=run_transform_data,
    dag=dag,
)

# Define task dependencies
extract_params_task >> [ingest_stations_task, ingest_trips_task] # pyright: ignore[reportUnusedExpression]
[ingest_stations_task, ingest_trips_task] >> transform_data_task # pyright: ignore[reportUnusedExpression]
