{{ config(materialized='view') }}

WITH legacy_trips_normalized AS (
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
        `start station name` as start_station_name,
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
        CAST(NULL AS STRING) as rideable_type,

        -- Debugging columns (preserve original station IDs)
        -- These will be fallback values in next step when we try to
        -- infer the post-2020 station id for the legacy trips
        `start station id` as legacy_start_station_id,
        `end station id` as legacy_end_station_id,

        -- Metadata
        'legacy' as data_source_schema,
        _ingested_at,
        _batch_key

    FROM {{ source('raw', 'citibike_trips_legacy') }}
),

legacy_trips_with_enriched_ids AS (
    SELECT
        t.ride_id,
        t.started_at,
        t.ended_at,
        t.trip_duration_seconds,
        CAST(COALESCE(s_start.short_name, t.legacy_start_station_id) AS STRING) AS start_station_id,
        t.start_station_name,
        CAST(COALESCE(s_end.short_name, t.legacy_end_station_id) AS STRING) AS end_station_id,
        t.end_station_name,
        t.start_lat,
        t.start_lng,
        t.end_lat,
        t.end_lng,
        t.member_casual,
        t.gender,
        t.bike_id,
        t.birth_year,
        t.rideable_type,
        t.legacy_start_station_id,
        t.legacy_end_station_id,
        t.data_source_schema,
        t._ingested_at,
        t._batch_key

    FROM
        legacy_trips_normalized t
    LEFT JOIN
        {{ ref('stations') }} s_start
    ON
        LOWER(TRIM(t.start_station_name)) = LOWER(TRIM(s_start.name))
    LEFT JOIN
        {{ ref('stations') }} s_end
    ON
        LOWER(TRIM(t.end_station_name)) = LOWER(TRIM(s_end.name))
)

SELECT * FROM legacy_trips_with_enriched_ids