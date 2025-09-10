import os
from citibike.config import load_env_config
from citibike.ingestion.borough_boundaries import ingest_borough_boundaries
from citibike.dbt import run_dbt_command

def run():
    # Load environment configuration
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)
    
    # Temporary shim for ingest_borough_boundaries that expects config dict
    config = {
        'GOOGLE_APPLICATION_CREDENTIALS': os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
        'GCP_PROJECT_ID': os.environ.get('GCP_PROJECT_ID'),
        'BQ_DATASET': os.environ.get('BQ_DATASET'),
        'NYC_BOROUGH_BOUNDARY_URL': os.environ.get('NYC_BOROUGH_BOUNDARY_URL')
    }

    # Dry run mode - validate configuration and exit
    if os.environ.get('CITIBIKE_DRY_RUN', '').lower() == 'true':
        print("✅ DRY RUN: Configuration loaded successfully")
        print(f"   Environment: {env_name}")
        print(f"   GCP_PROJECT_ID: {os.environ.get('GCP_PROJECT_ID', 'NOT SET')}")
        print(f"   BQ_DATASET: {os.environ.get('BQ_DATASET', 'NOT SET')}")
        print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'NOT SET')}")
        print(f"   GBFS_STATION_URL: {os.environ.get('GBFS_STATION_URL', 'NOT SET')}")
        print(f"   TRIP_DATA_URL: {os.environ.get('TRIP_DATA_URL', 'NOT SET')}")
        print("⏹️  Stopping here - no data ingestion or dbt operations performed")
        return
    
    print("Stage 1: Ingest official borough boundaries")
    ingest_borough_boundaries(config)

    print("Stage 2: Transform to silver layer")
    dbt_command = "dbt run --select +silver_nyc_borough_boundaries"
    run_dbt_command(dbt_command.split())


if __name__ == "__main__":
    run()