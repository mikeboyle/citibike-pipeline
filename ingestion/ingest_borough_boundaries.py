from typing import Any, Dict
import requests
import json
from datetime import datetime, timezone

from utils.config import load_config
from utils.bigquery import initialize_bigquery_client

def ingest_borough_boundaries(config: Dict[str, Any]):
    """Fetch NYC borough boundaries and load to BigQuery raw table"""
    
    url = config['NYC_BOROUGH_BOUNDARY_URL']
    print(f"Fetching data from {url}...")

    response = requests.get(url)
    response.raise_for_status()
    geojson_data = response.json()

    print(f"Data received from {url}...")


    # Prepare rows for BigQuery (one row per borough)
    rows = []
    for feature in geojson_data["features"]:
        rows.append({
            "borough_code": int(feature["properties"]["BoroCode"]),
            "borough_name": feature["properties"]["BoroName"],
            "feature_geojson": json.dumps(feature), # Store complete feature
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
        })
    
    print(f"Prepared {len(rows)} borough boundaries for ingestion")

    # Insert to BigQuery
    client = initialize_bigquery_client(config)
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BQ_DATASET_RAW']}.nyc_borough_boundaries"

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    else:
        print(f"Successfully inserted {len(rows)} rows to {table_id}")

# TODO: Remove after development
if __name__ == "__main__":
    config = load_config("dev")
    ingest_borough_boundaries(config)

