import os
import json
from datetime import datetime, timezone

from citibike.database.bigquery import initialize_bigquery_client

def ingest_borough_boundaries():
    """Load NYC borough boundaries from local JSON file to BigQuery raw table"""

    json_file_path = os.path.join(os.path.dirname(__file__), "../data/nyc_borough_boundaries.json")

    with open(json_file_path, "r") as f:
        boundaries_data = json.load(f)

    print(f"Loaded {len(boundaries_data['features'])} borough boundaries from local file")

    # Prepare rows for BigQuery (one row per borough)
    rows = []
    batch_key = datetime.now(timezone.utc).isoformat()
    for feature in boundaries_data["features"]:
        rows.append({
            "borough_code": int(feature["properties"]["BoroCode"]),
            "borough_name": feature["properties"]["BoroName"],
            "feature_geojson": json.dumps(feature), # Store complete feature
            "_ingested_at": batch_key,
        })
    
    print(f"Prepared {len(rows)} borough boundaries for ingestion")

    # Insert to BigQuery
    client = initialize_bigquery_client()
    table_id = f"{os.environ['GCP_PROJECT_ID']}.{os.environ['BQ_DATASET']}.raw_nyc_borough_boundaries"

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    else:
        print(f"Successfully inserted {len(rows)} rows to {table_id}")


