CREATE OR REPLACE TABLE `{project_id}.{dataset_name}.citibike_stations` (
  station_data STRING,
  api_last_updated TIMESTAMP,
  api_version STRING,
  _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(_ingested_at);