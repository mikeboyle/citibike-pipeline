{{ config(
    materialized='incremental',
    unique_key='ride_id',
    partition_by={
        'field': 'started_at',
        'data_type': 'datetime',
        'granularity': 'day'
    },
    cluster_by=['start_station_id', 'started_at']
) }}

WITH unified_base AS (
    SELECT 
        ride_id,
        started_at,
        ended_at,
        trip_duration_seconds,
        {{ normalize_station_id('start_station_id') }} as start_station_id,
        start_station_name,
        {{ normalize_station_id('end_station_id') }} as end_station_id,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        gender,
        bike_id,
        birth_year,
        rideable_type,
        legacy_start_station_id,
        legacy_end_station_id,
        data_source_schema,
        _ingested_at,
        _batch_key
    FROM {{ ref('stg_trips_legacy') }}
    {% if is_incremental() %}
    WHERE _batch_key LIKE '{{ var("month_key") }}%'
    {% endif %}

    UNION ALL

    SELECT 
        ride_id,
        started_at,
        ended_at,
        trip_duration_seconds,
        {{ normalize_station_id('start_station_id') }} as start_station_id,
        start_station_name,
        {{ normalize_station_id('end_station_id') }} as end_station_id,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        gender,
        bike_id,
        birth_year,
        rideable_type,
        legacy_start_station_id,
        legacy_end_station_id,
        data_source_schema,
        _ingested_at,
        _batch_key
    FROM {{ ref('stg_trips_current') }}
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
        {{ derive_season('started_at') }} as start_season,

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
),

duplicate_ride_ids AS (
    SELECT ride_id
    FROM trips_with_geography
    GROUP BY ride_id
    HAVING COUNT(*) > 1
),

trips_with_data_quality_flags AS (
    SELECT
        t.*,
        -- flag for temporal outliers
        CASE WHEN
            t.trip_duration_seconds < 60 OR
            t.trip_duration_seconds > (60 * 60 * 24) OR
            t.started_at > {{ now_nyc_datetime() }} OR
            t.started_at < {{ citibike_inception_date() }} OR
            ended_at < started_at
        THEN TRUE ELSE FALSE END AS is_temporal_outlier,

        -- flag for geographic issues (impossible lat/lng or null lat/lng)
        (t.start_lat IS NULL OR
        t.start_lng IS NULL OR
        t.end_lat IS NULL OR
        t.end_lng IS NULL OR
        {{ is_geographic_outlier('t.start_lat', 't.start_lng') }} OR
        {{ is_geographic_outlier('t.end_lat', 't.end_lng') }}) AS is_geography_quality_issue,

        -- flag for data integrity issues
        CASE WHEN
            t.start_station_id IS NULL OR
            t.end_station_id IS NULL OR
            t.ride_id LIKE '%test%'
        THEN TRUE ELSE FALSE END AS is_data_integrity_issue,

        -- flag for potential duplicates
        (d.ride_id IS NOT NULL) AS is_duplicate_ride

    FROM trips_with_geography t
    LEFT JOIN duplicate_ride_ids d
    ON t.ride_id = d.ride_id
)

SELECT * FROM trips_with_data_quality_flags
