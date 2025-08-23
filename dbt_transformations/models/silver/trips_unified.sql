{{ config(
    materialized='incremental',
    unique_key='ride_id',
    partition_by={
        'field': 'started_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['start_station_id', 'started_at']
) }}

WITH unified_base AS (
    SELECT * FROM {{ ref('stg_trips_legacy') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_trips_current') }}
)

-- Final SELECT with enrichments
SELECT
    *,

    -- Temporal enrichments
    EXTRACT(HOUR FROM started_at) as start_hour,
    EXTRACT(DAYOFWEEK FROM started_at) as start_day_of_week,
    EXTRACT(MONTH FROM started_at) as start_month,
    CASE
        WHEN EXTRACT(MONTH FROM started_at) IN (12,1,2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM started_at) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM started_at) IN (6,7,8) THEN 'Summer'
        ELSE 'Fall'
    END as start_season,

    -- Boolean flags
    EXTRACT(DAYOFWEEK FROM started_at) IN (1,7) as is_weekend,
    start_station_id = end_station_id as is_round_trip

FROM unified_base