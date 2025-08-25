from citibike.database.bigquery import initialize_bigquery_client
from citibike.database.staging import StagingTableLoader

import requests
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
import pandas as pd

def _extract_station_rows(res: requests.Response, batch_key_value: str) -> List[Dict[str, Any]]:
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
                "station_id": str(station_id),
                "station_data": json.dumps(station),
                "api_last_updated": api_last_updated.isoformat(),
                "api_version": str(api_version),
                "_ingested_at": batch_key_value,
            })
    return rows

def ingest_station_data(config: Dict[str, Any], batch_date: datetime) -> None:
    # Fetch latest station data
    station_url = config['GBFS_STATION_URL']
    res = requests.get(station_url)
    res.raise_for_status()

    # Extract rows from response (one station = one row)
    batch_key_value = batch_date.isoformat()
    rows = _extract_station_rows(res, batch_key_value)
    print(f"found {len(rows)} stations")

    # Convert to dataframe and cast columns
    df = pd.DataFrame(rows)
    
    # Cast to match BigQuery schema
    df['station_id'] = df['station_id'].astype(str)
    df['station_data'] = df['station_data'].astype(str) 
    df['api_last_updated'] = pd.to_datetime(df['api_last_updated'])
    df['api_version'] = df['api_version'].astype(str)
    df['_ingested_at'] = pd.to_datetime(df['_ingested_at'])

    # Initialize BigQuery client 
    client = initialize_bigquery_client(config)

    # Build table reference
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BQ_DATASET_RAW']}.citibike_stations"

    # Insert the rows
    loader = StagingTableLoader(client, table_id, "_ingested_at")
    loader.load_and_merge_df(df, batch_key_value)
    
    print(f"Successfully inserted {len(rows)} station records")
