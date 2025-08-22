import os
import pandas as pd
from typing import Any, Dict

from utils.validation import add_metadata_columns, validate_and_cast_trip_schema
from utils.schemas import CURRENT_TRIP_CSV_SCHEMA, LEGACY_TRIP_CSV_SCHEMA
from utils.storage import LocalStorage
from utils.trips import TripDataDownloader
from utils.staging import StagingTableLoader
from utils.config import load_config
from utils.bigquery import initialize_bigquery_client


def _ingest_trip_data(config: Dict[str, Any], year: int, month: int, table_name: str, schema: Dict[str, Any]):
    """Download and ingest trip data for the given month, table, and schema."""
    # Initialize components
    storage = LocalStorage()
    client = initialize_bigquery_client(config)
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BQ_DATASET_RAW']}.{table_name}"
    loader = StagingTableLoader(client, table_id, "_batch_key")

    # Download and extract CSV files
    downloader = TripDataDownloader(storage, config["TRIP_DATA_URL"])
    csv_paths = downloader.download_month(year, month)
    print(f"Downloaded CSV files to paths {csv_paths}")

    # Process each CSV file as a separate batch
    for csv_path in csv_paths:
        batch_key = _extract_batch_key_from_filename(csv_path)
        _process_csv_batch(csv_path, batch_key, loader, schema)

    # Clean up downloaded files
    storage.cleanup(csv_paths)

    print(f"Successfully ingested trip data for {year}-{month:02d}")


def ingest_legacy_trip_data(config: Dict[str, Any], year: int, month: int) -> None:
    """Download and ingest trip data for the given month, expecting the legacy schema."""
    _ingest_trip_data(config, year, month, "citibike_trips_legacy", LEGACY_TRIP_CSV_SCHEMA)


def ingest_current_trip_data(config: Dict[str, Any], year: int, month: int) -> None:
    """Download and ingest trip data for the given month."""
    _ingest_trip_data(config, year, month, "citibike_trips_current", CURRENT_TRIP_CSV_SCHEMA)


def _extract_batch_key_from_filename(csv_path: str) -> str:
    filename = os.path.basename(csv_path)
    
    # "202401-citibike-tripdata_1.csv" -> ["202401", "citibike", "tripdata_1.csv"]
    parts = filename.split("-")
    year_month = parts[0]
    batch_part = parts[-1].split('.')[0] # "tripdata_1.csv" -> "tripdata_1"
    batch_num = batch_part.split("_")[-1]

    year = year_month[:4]
    month = year_month[4:6]

    return f"{year}-{month}-{batch_num}"

def _process_csv_batch(csv_path: str, batch_key_val: str, loader: StagingTableLoader, schema: Dict[str, Any]):
    print(f"processing csv at path {csv_path}, batch_key_value = {batch_key_val}")
    df_raw = pd.read_csv(csv_path)

    # TEMPORARY: Limit to first 100 rows for testing
    df_raw = df_raw.head(100)
    print(f"DEBUG: Limited to {len(df_raw)} rows for testing")

    df_validated = validate_and_cast_trip_schema(df_raw, schema)
    print(f"validation complete!")

    df_metadata = add_metadata_columns(df_validated, batch_key_val)
    print(f"added metadata!")

    loader.load_and_merge_df(df_metadata, batch_key_val)

# TODO: remove this after development
if __name__ == "__main__":
    # Months to test:
    # - 2024-01: new filename pattern: YYYYMM-citibike-tripdata.zip, contians YYYYMM-citibike-tripdata_N.csv
    # - 2022-08: old pattern: YYYY-citibike-tripdata.zip, contains YYYYMM-citibike-tripdata.zip, which has YYYYMM-citibike-tripdata_N.csv
    # - 2019-06: old pattern: YYYY-citibike-tripdata.zip, contains 06_June which has YYYYMM-citibike-tripdata_N.csv
    year, month = 2020, 1
    config = load_config("dev")

    print(f"Starting ingestion for {year}-{month:02d}")
    # ingest_legacy_trip_data(config, year, month)
    ingest_current_trip_data(config, year, month)
    print("Ingestion completed successfully!")
