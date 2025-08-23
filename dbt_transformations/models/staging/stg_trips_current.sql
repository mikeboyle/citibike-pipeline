{{ config(materialized='view') }}

SELECT 
    -- Keep existing ride_id
    ride_id,
    
    -- Timing (calculate duration from timestamps)
    started_at,
    ended_at, 
    TIMESTAMP_DIFF(ended_at, started_at, SECOND) as trip_duration_seconds,
    
    -- Station information (already in correct format)
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    
    -- Coordinates (already in correct format)
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    
    -- User information
    member_casual,
    
    -- Legacy-only columns (NULL for current)
    NULL as gender,
    NULL as bike_id,
    NULL as birth_year,

    -- Current-only columns
    rideable_type,
    
    -- Debugging columns (NULL for current)
    NULL as legacy_start_station_id,
    NULL as legacy_end_station_id,
    
    -- Metadata
    'current' as data_source_schema,
    _ingested_at,
    _batch_key
    
FROM {{ source('raw', 'citibike_trips_current') }}