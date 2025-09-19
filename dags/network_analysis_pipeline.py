import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from citibike.config import load_env_config
from citibike.dbt import run_dbt_command
from citibike.networks.analysis import CommuterNetworkAnalyzer

def transform_edges_task(**context):
    # Load environment configuration
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)

    print("Calculating edges for 90 day commuter network")

    edges_dbt_command = "dbt run --select gold_commuter_edges"
    run_dbt_command(edges_dbt_command.split())

def transform_nodes_task(**context):
    # Load environment configuration
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)

    print("Calculating nodes for 90 day commuter network")
    nodes_dbt_command = "dbt run --select gold_commuter_hubs"
    run_dbt_command(nodes_dbt_command.split())

def analyze_network_task(**context):
    # Load environment configuration
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)

    print("Analyzing commuter network")

    analyzer = CommuterNetworkAnalyzer()

    print("Extracting network data from BQ")
    hubs_df, edges_df = analyzer.extract_network_data()

    print(f"Loaded {len(hubs_df)} stations and {len(edges_df)} edges")

    print("Running network flow analysis")

    results_df = analyzer.run_analysis(hubs_df, edges_df)
    print(f"Found {len(results_df)} critical or bottleneck nodes")
    print(results_df.dtypes)
    print(results_df[['name', 'borough', 'is_critical', 'is_bottleneck', 'pagerank_score']].sort_values('pagerank_score', ascending=False).head(10))

    print("Writing analysis results to database")
    table_name = "gold_network_node_analysis"
    analyzer.write_results_to_bq(results_df, table_name)

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
    "network_analysis_pipeline",
    default_args=default_args,
    description="Analyze morning commute network for past 90 days of data",
    schedule_interval=None, # Manual trigger for now
    catchup=False, # Don't run for past dates
    tags=["citibike"],
)

# Define tasks
task_transform_edges = PythonOperator(
    task_id="transform_edges",
    python_callable=transform_edges_task,
    dag=dag,
)

task_transform_nodes = PythonOperator(
    task_id="transform_nodes",
    python_callable=transform_nodes_task,
    dag=dag,
)

task_analyze_network = PythonOperator(
    task_id="analyze_network",
    python_callable=analyze_network_task,
    dag=dag
)

task_transform_edges >> task_transform_nodes >> task_analyze_network # pyright: ignore[reportUnusedExpression]
