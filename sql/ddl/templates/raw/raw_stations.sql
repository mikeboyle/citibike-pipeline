CREATE OR REPLACE TABLE `{project_id}.{dataset_name}.raw_stations{suffix}` (
  station_id STRING,
  station_data STRING,
  api_last_updated DATETIME,
  api_version STRING,
  _ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP("America/New_York")
)
PARTITION BY DATE(_ingested_at);