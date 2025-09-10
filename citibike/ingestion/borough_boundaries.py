import os
from typing import Any, Dict
import requests
import json
from datetime import datetime, timezone

from citibike.database.bigquery import initialize_bigquery_client

def ingest_borough_boundaries():
    """Fetch NYC borough boundaries and load to BigQuery raw table"""
    
    url = os.environ['NYC_BOROUGH_BOUNDARY_URL']
    print(f"Fetching data from {url}...")

    response = requests.get(url)
    response.raise_for_status()
    geojson_data = response.json()

    print(f"Data received from {url}...")


    # Prepare rows for BigQuery (one row per borough)
    rows = []
    batch_key = datetime.now(timezone.utc).isoformat()
    for feature in geojson_data["features"]:
        rows.append({
            "borough_code": int(feature["properties"]["BoroCode"]),
            "borough_name": feature["properties"]["BoroName"],
            "feature_geojson": json.dumps(feature), # Store complete feature
            "_ingested_at": batch_key,
        })
    
    print(f"Prepared {len(rows)} borough boundaries for ingestion")

    # Insert to BigQuery
    client = initialize_bigquery_client(validate_connection=True)
    table_id = f"{os.environ['GCP_PROJECT_ID']}.{os.environ['BQ_DATASET']}.raw_nyc_borough_boundaries"

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    else:
        print(f"Successfully inserted {len(rows)} rows to {table_id}")


