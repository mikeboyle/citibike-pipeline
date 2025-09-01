CREATE OR REPLACE TABLE `{project_id}.{dataset_name}.raw_trips_legacy{suffix}` (
  tripduration INT64,                    -- Trip duration in seconds
  starttime DATETIME,                   -- Start time and date
  stoptime DATETIME,                    -- Stop time and date
  `start station id` STRING,              -- Start station ID
  `start station name` STRING,           -- Start station name
  `start station latitude` FLOAT64,     -- Start station latitude
  `start station longitude` FLOAT64,    -- Start station longitude
  `end station id` STRING,                -- End station ID
  `end station name` STRING,             -- End station name
  `end station latitude` FLOAT64,       -- End station latitude
  `end station longitude` FLOAT64,      -- End station longitude
  bikeid INT64,                          -- Bike ID
  usertype STRING,                       -- Customer or Subscriber
  `birth year` INT64,                    -- Year of birth (nullable)
  gender INT64,                          -- 0=unknown, 1=male, 2=female (nullable)
  _ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP("America/New_York"),
  _batch_key STRING                      -- YYYY-MM-batch_num
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY `start station id`, starttime;