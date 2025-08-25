from citibike.config import load_config
from citibike.ingestion.borough_boundaries import ingest_borough_boundaries
from citibike.dbt import run_dbt_command

if __name__ == "__main__":
    config = load_config("dev") # TODO: make this a parameter

    print("Stage 1: Ingest official borough boundaries")
    ingest_borough_boundaries(config)

    print("Stage 2: Transform to silver layer")
    dbt_command = "dbt run --select +silver_nyc_borough_boundaries"
    run_dbt_command(dbt_command.split())