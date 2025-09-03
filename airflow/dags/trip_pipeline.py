import sys
import json

from citibike.config import load_config
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

    # run the pipeline
    config = load_config("dev") # hard code this for now TODO: make env a argument

    print(f"Stage 1: Ingest station data from API")
    ingestion_date = now_nyc_datetime()
    ingest_station_data(config, ingestion_date)

    print(f"Stage 2: Ingest trip data for {month_key}")
    ingest_trip_data(config, year, month)

    print(f"Stage 3: Transform raw stations/trips to silver stations/trips")
    dbt_vars = json.dumps({ "month_key": month_key })
    dbt_command = ["dbt", "run", "--select", "+silver_trips", "--vars", dbt_vars]
    run_dbt_command(dbt_command)


if __name__ == "__main__":
    run()
