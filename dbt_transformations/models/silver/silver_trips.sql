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
    {% if is_incremental() %}
    WHERE _batch_key LIKE '{{ var("month_key") }}%'
    {% endif %}

    UNION ALL

    SELECT * FROM {{ ref('stg_trips_current') }}
    {% if is_incremental() %}
    WHERE _batch_key LIKE '{{ var("month_key") }}%'
    {% endif %}
),

trips_enriched AS (
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
),

trips_with_geography AS (
    SELECT
        t.*,
        start_station.borough AS start_borough,
        end_station.borough AS end_borough

    FROM trips_enriched t

    LEFT JOIN {{ ref('silver_stations') }} start_station
        ON t.start_station_id = start_station.short_name

    LEFT JOIN {{ ref('silver_stations') }} end_station
        ON t.end_station_id = end_station.short_name
)

SELECT * FROM trips_with_geography
