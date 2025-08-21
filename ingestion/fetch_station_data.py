from utils.config import load_config
from utils.bigquery import initialize_bigquery_client

import requests
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

def extract_station_rows(res: requests.Response) -> List[Dict[str, Any]]:
    res_json = res.json()
    api_last_updated = datetime.fromtimestamp(res_json.get('last_updated'), tz=timezone.utc)
    api_version = res_json.get('version')
    
    data = res_json.get('data', {})
    stations = data.get('stations', [])
    
    rows = []
    for station in stations:
        station_id = station.get("station_id")
        if not station_id:
            print(f"Cannot ingest station; station_id field missing. {station}")
        else:
            rows.append({
                "station_id": station_id,
                "station_data": json.dumps(station),
                "api_last_updated": api_last_updated.isoformat(),
                "api_version": api_version,
                "_ingested_at": datetime.now(timezone.utc).isoformat(),
            })
    return rows

def run(config: Dict[str, Any]):
    # Fetch latest station data
    station_url = config['GBFS_STATION_URL']
    res = requests.get(station_url)
    res.raise_for_status()

    rows = extract_station_rows(res)
    print(f"found {len(rows)} stations")

    # Initialize BigQuery client 
    client = initialize_bigquery_client(config)

    # Build table reference
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BQ_DATASET_RAW']}.citibike_stations"

    # Insert the rows
    errors = client.insert_rows_json(table_id, rows)

    # Handle any errors
    if errors:
        raise Exception(f"BigQuery insertion failed: {errors}")
    
    print(f"Successfully inserted {len(rows)} station records")


# TODO: remove this after development
if __name__ == "__main__":
    config = load_config("dev")
    run(config)
