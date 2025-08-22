import os
import pandas as pd
from typing import Any, Dict

from utils.validation import add_metadata_columns, validate_and_cast_trip_schema
from utils.schemas import CURRENT_TRIP_CSV_SCHEMA
from utils.storage import LocalStorage, StorageLocation
from utils.trips import TripDataDownloader
from utils.staging import StagingTableLoader
from utils.config import load_config
from utils.bigquery import initialize_bigquery_client


def ingest_trip_data(year: int, 
                     month: int, 
                     storage: StorageLocation, 
                     schema: Dict[str, Any],
                     staging_loader: StagingTableLoader
                     ) -> None:
    """
    Download and ingest trip data for the given month.

    Raises:
        Exception on any failure

    """
    downloader = TripDataDownloader(storage)

    # Download and extract CSV files
    csv_paths = downloader.download_month(year, month)
    print(f"Downloaded CSV files to paths {csv_paths}")

    # Process each CSV file as a separate batch
    for csv_path in csv_paths:
        batch_key = _extract_batch_key_from_filename(csv_path)
        _process_csv_batch(csv_path, schema, batch_key, staging_loader)
    
    # Clean up downloaded files
    # storage.cleanup(csv_paths)

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

def _process_csv_batch(csv_path: str, schema: Dict[str, Any], batch_key_val: str, staging_loader: StagingTableLoader):
    print(f"processing csv at path {csv_path}, batch_key = {batch_key_val}")
    df_raw = pd.read_csv(csv_path)

    # TEMPORARY: Limit to first 100 rows for testing
    df_raw = df_raw.head(100)
    print(f"DEBUG: Limited to {len(df_raw)} rows for testing")

    df_validated = validate_and_cast_trip_schema(df_raw, schema)
    print(f"validation complete!")

    df_metadata = add_metadata_columns(df_validated, batch_key_val)
    print(f"added metadata!")

    staging_loader.load_and_merge_df(df_metadata, batch_key_val)


if __name__ == "__main__":
    year, month = 2024, 1
    storage = LocalStorage()
    config = load_config("dev")
    client = initialize_bigquery_client(config)
    # Build table reference
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BQ_DATASET_RAW']}.citibike_trips_current"
    loader = StagingTableLoader(client, table_id, "_batch_key", "ride_id")

    print(f"Starting ingestion for {year}-{month:02d}")
    ingest_trip_data(year, month, storage, CURRENT_TRIP_CSV_SCHEMA, loader)
    print("Ingestion completed successfully!")
