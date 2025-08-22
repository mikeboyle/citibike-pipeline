from typing import Any, Dict

# What we expect from Citibike CSV files (2020+)
CURRENT_TRIP_CSV_SCHEMA: Dict[str, Any] = {
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

# What we expect from Citibike CSV files (pre 2020)
LEGACY_TRIP_CSV_SCHEMA: Dict[str, Any] = {
    'tripduration': 'Int64',
    'starttime': 'datetime64[ns]',
    'stoptime': 'datetime64[ns]',
    'start station id': 'string',
    'start station name': 'string',
    'start station latitude': 'float64',
    'start station longitude': 'float64',
    'end station id': 'string',
    'end station name': 'string',
    'end station latitude': 'float64',
    'end station longitude': 'float64',
    'bikeid': 'Int64',
    'usertype': 'string',
    'birth year': 'Int64',
    'gender': 'Int64',
}

# What we store in BigQuery (CSV + our metadata)
LEGACY_TRIP_BQ_SCHEMA = {
    **LEGACY_TRIP_CSV_SCHEMA,
    '_ingested_at': 'datetime64[ns]',
    '_batch_key': 'string',
}