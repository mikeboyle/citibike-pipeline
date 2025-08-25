CREATE OR REPLACE TABLE `{project_id}.{dataset_name}.nyc_borough_boundaries{suffix}` (
  borough_code INT64 NOT NULL,
  borough_name STRING NOT NULL,
  feature_geojson STRING NOT NULL,      -- Complete GEOJSON feature as string
  _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY borough_code;