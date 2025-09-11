"""
A very simple Airflow DAG to learn the basic principles.

This DAG demonstrates:
1. Basic DAG structure and configuration
2. Simple PythonOperator tasks
3. Task dependencies 
4. Error handling and retries
5. Manual triggering (no schedule)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

# Default arguments for all tasks in this DAG
default_args = {
    'owner': 'citibike-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,  # Set to True and add email for notifications
    'email_on_retry': False,
    'retries': 2,  # Retry failed tasks 2 times
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Define the DAG
dag = DAG(
    'hello_citibike',
    default_args=default_args,
    description='A simple tutorial DAG for CitiBike project',
    schedule_interval=None,  # Manual trigger only (no automatic scheduling)
    catchup=False,  # Don't run for past dates
    tags=['tutorial', 'citibike'],
)

def hello_world():
    """Simple function that prints hello world."""
    print("Hello from CitiBike Airflow! 🚲")
    print(f"Current environment: {os.environ.get('CITIBIKE_ENV', 'not-set')}")
    return "Hello task completed successfully"

def test_citibike_import():
    """Test that we can import our citibike package."""
    try:
        from citibike.config import load_env_config
        print("✅ Successfully imported citibike.config")
        
        # Try loading config
        env_name = os.environ.get('CITIBIKE_ENV', 'dev')
        load_env_config(env_name)
        print(f"✅ Successfully loaded config for environment: {env_name}")
        
        # Show some config values
        print(f"Project ID: {os.environ.get('GCP_PROJECT_ID', 'NOT SET')}")
        print(f"BQ Dataset: {os.environ.get('BQ_DATASET', 'NOT SET')}")
        
        return "Config test completed successfully"
    except Exception as e:
        print(f"❌ Error testing citibike import: {e}")
        raise  # This will cause the task to fail

def simulate_data_processing():
    """Simulate some data processing work."""
    import time
    print("🔄 Starting data processing simulation...")
    time.sleep(3)  # Simulate some work
    print("📊 Processing 1000 records...")
    time.sleep(2)  # More simulation
    print("✅ Data processing completed successfully")
    return "Data processing simulation completed"

# Define tasks
task_hello = PythonOperator(
    task_id='say_hello',
    python_callable=hello_world,
    dag=dag,
)

task_test_config = PythonOperator(
    task_id='test_citibike_config',
    python_callable=test_citibike_import,
    dag=dag,
)

task_process_data = PythonOperator(
    task_id='simulate_data_processing',
    python_callable=simulate_data_processing,
    dag=dag,
)

# Simple bash task to show environment
task_show_env = BashOperator(
    task_id='show_environment',
    bash_command='echo "Current directory: $(pwd)" && echo "Python version: $(python --version)" && echo "Environment variables:" && env | grep CITIBIKE || echo "No CITIBIKE env vars found"',
    dag=dag,
)

# Define task dependencies
# This creates the flow: hello -> test_config -> process_data
#                                           -> show_env
task_hello >> task_test_config >> [task_process_data, task_show_env]