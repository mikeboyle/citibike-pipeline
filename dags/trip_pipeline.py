import sys
import json
import os

from citibike.config import load_env_config
from citibike.ingestion.trips import ingest_trip_data
from citibike.ingestion.stations import ingest_station_data
from citibike.dbt import run_dbt_command
from citibike.utils.date_helpers import now_nyc_datetime


def run():
    """
    Ingests latest raw stations and trip data, then transforms to silver
    stations and silver trips.
    """
    # validate command line args
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python trip_pipeline <year> <month>")
    
    year_arg, month_arg = sys.argv[1], sys.argv[2]

    try:
        year = int(year_arg)
        month = int(month_arg)
        month_key = f"{year:04d}-{month:02d}"

    except ValueError:
        raise SystemExit(f"Usage: python trip_pipeline <year> <month>, got: {year_arg} for year and {month_arg} for month")

    # Load environment configuration
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)
    
    # Dry run mode - validate configuration and exit
    if os.environ.get('CITIBIKE_DRY_RUN', '').lower() == 'true':
        print("✅ DRY RUN: Configuration loaded successfully")
        print(f"   Environment: {env_name}")
        print(f"   Year/Month: {year}/{month} (month_key: {month_key})")
        print(f"   GCP_PROJECT_ID: {os.environ.get('GCP_PROJECT_ID', 'NOT SET')}")
        print(f"   BQ_DATASET: {os.environ.get('BQ_DATASET', 'NOT SET')}")
        print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'NOT SET')}")
        print(f"   GBFS_STATION_URL: {os.environ.get('GBFS_STATION_URL', 'NOT SET')}")
        print(f"   TRIP_DATA_URL: {os.environ.get('TRIP_DATA_URL', 'NOT SET')}")
        
        # Test BigQuery connection
        try:
            from citibike.database.bigquery import initialize_bigquery_client
            initialize_bigquery_client(validate_connection=True)
        except Exception as e:
            print(f"❌ BigQuery connection validation failed: {e}")
            return
        
        print("⏹️  Stopping here - no data ingestion or dbt operations performed")
        return
    
    print(f"Stage 1: Ingest station data from API")
    ingestion_date = now_nyc_datetime()
    ingest_station_data(ingestion_date)

    print(f"Stage 2: Ingest trip data for {month_key}")
    ingest_trip_data(year, month)

    print(f"Stage 3: Transform data through gold dashboard models")
    dbt_vars = json.dumps({ "month_key": month_key })
    dbt_command = ["dbt", "run", "--selector", "dashboard_models", "--vars", dbt_vars]
    run_dbt_command(dbt_command)


if __name__ == "__main__":
    run()
