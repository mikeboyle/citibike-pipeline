from datetime import datetime, timezone

from citibike.config import load_config
from citibike.ingestion.stations import ingest_station_data
from citibike.dbt import run_dbt_command

if __name__ == "__main__":
    config = load_config("dev")
    ingestion_date = datetime.now(timezone.utc)

    print("Stage 1: Ingest latest GBFS stations data and save to raw")
    ingest_station_data(config, ingestion_date)

    print("Stage 2: Transform row stations to silver stations")
    run_dbt_command("dbt run --select +silver_stations")
