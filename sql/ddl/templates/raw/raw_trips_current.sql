CREATE OR REPLACE TABLE `{project_id}.{dataset_name}.raw_trips_current{suffix}` (
  ride_id STRING,                        -- Unique ride identifier
  rideable_type STRING,                  -- classic_bike, electric_bike, docked_bike
  started_at DATETIME,                  -- Start time and date
  ended_at DATETIME,                    -- End time and date
  start_station_name STRING,             -- Start station name
  start_station_id STRING,               -- Start station ID (string in current format)
  end_station_name STRING,               -- End station name
  end_station_id STRING,                 -- End station ID (string in current format)
  start_lat FLOAT64,                     -- Start latitude
  start_lng FLOAT64,                     -- Start longitude
  end_lat FLOAT64,                       -- End latitude
  end_lng FLOAT64,                       -- End longitude
  member_casual STRING,                  -- member or casual
  _ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP("America/New_York"),
  _batch_key STRING                      -- YYYY-MM-batch_num
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY start_station_id, started_at;