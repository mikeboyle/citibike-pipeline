import os
from citibike.database.bigquery import initialize_bigquery_client
from citibike.database.staging import StagingTableLoader
from citibike.utils.date_helpers import DATETIME_STR_FORMAT

import requests
import json
from datetime import datetime
from typing import Any, Dict, List
import pandas as pd

def _extract_station_rows(res: requests.Response, batch_key_value: pd.Timestamp) -> List[Dict[str, Any]]:
    res_json = res.json()
    api_last_updated = pd.Timestamp.fromtimestamp(res_json.get('last_updated')).tz_localize(None)
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
                "api_last_updated": api_last_updated,
                "api_version": str(api_version),
                "_ingested_at": batch_key_value,
            })
    return rows

def ingest_station_data(batch_date: datetime) -> None:
    # Fetch latest station data
    station_url = os.environ['GBFS_STATION_URL']
    res = requests.get(station_url)
    res.raise_for_status()

    # Extract rows from response (one station = one row)
    batch_key_value = pd.Timestamp(batch_date).tz_localize(None)
    rows = _extract_station_rows(res, batch_key_value)
    print(f"found {len(rows)} stations")

    # Convert to dataframe and cast columns
    df = pd.DataFrame(rows)
    
    # Cast to match BigQuery schema
    df['station_id'] = df['station_id'].astype(str)
    df['station_data'] = df['station_data'].astype(str) 
    df['api_version'] = df['api_version'].astype(str)

    # Initialize BigQuery client 
    client = initialize_bigquery_client(validate_connection=True)

    # Build table reference
    table_id = f"{os.environ['GCP_PROJECT_ID']}.{os.environ['BQ_DATASET']}.raw_stations"

    # Insert the rows
    loader = StagingTableLoader(client, table_id, "_ingested_at")
    loader.load_and_merge_df(df, batch_key_value.strftime(DATETIME_STR_FORMAT))
    
    print(f"Successfully inserted {len(rows)} station records")
