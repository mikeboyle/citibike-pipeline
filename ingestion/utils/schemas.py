from pandas.api.extensions import ExtensionDtype
from typing import Dict

# What we expect from Citibike CSV files (2020+)
CURRENT_TRIP_CSV_SCHEMA = {
    'ride_id': 'string',
    'rideable_type': 'string',
    'started_at': 'datetime64[ns]',
    'ended_at': 'datetime64[ns]',
    'start_station_name': 'string',
    'start_station_id': 'string',
    'end_station_name': 'string',
    'end_station_id': 'string',
    'start_lat': 'float64',
    'start_lng': 'float64',
    'end_lat': 'float64',
    'end_lng': 'float64',
    'member_casual': 'string',
}

# What we store in BigQuery (CSV + our metadata)
CURRENT_TRIP_BQ_SCHEMA = {
    **CURRENT_TRIP_CSV_SCHEMA,
    '_ingested_at': 'datetime64[ns]',
    '_batch_key': 'string',
}