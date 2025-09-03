from datetime import datetime, timezone

from citibike.config import load_config
from citibike.ingestion.stations import ingest_station_data
from citibike.dbt import run_dbt_command
from citibike.utils.date_helpers import now_nyc_datetime

if __name__ == "__main__":
    config = load_config("dev") # TODO: make this a parameter
    ingestion_date = now_nyc_datetime()

    print("Stage 1: Ingest latest GBFS stations data and save to raw")
    ingest_station_data(config, ingestion_date)

    print("Stage 2: Transform raw stations to silver stations")
    dbt_command = "dbt run --select +silver_stations"
    run_dbt_command(dbt_command.split())
