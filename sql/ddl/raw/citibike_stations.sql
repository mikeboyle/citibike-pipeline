--NOTE: replace project_id with your BigQuery project id
CREATE TABLE `{project_id}.raw.citibike_stations` (
  station_data STRING,                   -- Full JSON as string from GBFS feed
  api_last_updated TIMESTAMP,
  api_version STRING,
  _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(_ingested_at);