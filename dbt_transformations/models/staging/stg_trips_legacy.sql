{{ config(materialized='view') }}

SELECT 
    -- Generated deterministic ride_id
    CONCAT('legacy_', TO_HEX(MD5(CONCAT(
        CAST(starttime AS STRING),
        CAST(stoptime AS STRING), 
        CAST(`start station id` AS STRING),
        CAST(`end station id` AS STRING),
        CAST(bikeid AS STRING)
    )))) as ride_id,
    
    -- Standardized timing columns
    starttime as started_at,
    stoptime as ended_at,
    tripduration as trip_duration_seconds,

    -- Station information (standardized column names)
    `start station id` as start_station_id,
    `start station name` as start_station_name,
    `end station id` as end_station_id,
    `end station name` as end_station_name,

    -- Coordinates (standardized names to match current schema)
    `start station latitude` as start_lat,
    `start station longitude` as start_lng,
    `end station latitude` as end_lat,
    `end station longitude` as end_lng,

    -- User information transformation to member_casual format
    CASE usertype
        WHEN 'Subscriber' THEN 'member'
        WHEN 'Customer' THEN 'casual'
        ELSE NULL
    END as member_casual,

    CASE gender
        WHEN 0 THEN 'unknown'
        WHEN 1 THEN 'male'
        WHEN 2 THEN 'female'
        ELSE NULL
    END as gender,

    CAST(bikeid AS STRING) as bike_id,
    `birth year` as birth_year,

    -- Columns found only in current schema
    NULL as rideable_type,

    -- Debugging columns (preserve original station IDs)
    `start station id` as legacy_start_station_id,
    `end station id` as legacy_end_station_id,

    -- Metadata
    'legacy' as data_source_schema,
    _ingested_at,
    _batch_key
    
FROM {{ source('raw', 'citibike_trips_legacy') }}