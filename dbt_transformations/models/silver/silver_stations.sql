{{ config(materialized='table') }}

WITH parsed_stations AS (
    SELECT
        station_id,
        JSON_VALUE(station_data, '$.name') AS name,
        JSON_VALUE(station_data, '$.short_name') AS short_name,
        JSON_VALUE(station_data, '$.region_id') AS region_id,
        CAST(JSON_VALUE(station_data, '$.lat') AS FLOAT64) AS lat,
        CAST(JSON_VALUE(station_data, '$.lon') AS FLOAT64) AS lon,
        JSON_VALUE(station_data, '$.external_id') AS external_id,
        CAST(JSON_VALUE(station_data, '$.has_kiosk') AS BOOL) AS has_kiosk,
        CAST(JSON_VALUE(station_data, '$.eightd_has_key_dispenser') AS BOOL) AS eightd_has_key_dispenser,
        CAST(JSON_VALUE(station_data, '$.electric_bike_surcharge_waiver') AS BOOL) AS electric_bike_surcharge_waiver,
        CAST(JSON_VALUE(station_data, '$.capacity') AS INT64) AS capacity,
        JSON_VALUE(station_data, '$.station_type') AS station_type,
        station_data,
        api_last_updated,
        api_version,
        _ingested_at
    FROM {{ source('raw', 'raw_stations') }}

),

normalized_stations AS (
    SELECT
        station_id,
        name,
        {{ normalize_station_id('short_name') }} AS short_name,
        region_id,
        lat,
        lon,
        external_id,
        has_kiosk,
        eightd_has_key_dispenser,
        electric_bike_surcharge_waiver,
        capacity,
        station_type,
        station_data,
        api_last_updated,
        api_version,
        _ingested_at
    FROM parsed_stations
),

latest_stations AS (
    SELECT *
    FROM normalized_stations
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY station_id
            ORDER BY _ingested_at DESC
        ) = 1
),

deduplicated_stations AS (
    SELECT *
    FROM latest_stations
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(name))
        ORDER BY station_id
    ) = 1
),

max_ingestion_time AS (
    SELECT MAX(_ingested_at) as max_ingested_at
    FROM latest_stations
),

stations_with_boroughs AS (
    SELECT
        l.*,
        COALESCE(
            bb.borough_name,
            (
                CASE WHEN l.region_id IN ('70', '311') THEN 'NJ'
                ELSE 'Unknown' END
            )
        ) AS borough,
        l._ingested_at = (SELECT max_ingested_at FROM max_ingestion_time) AS is_active,
    FROM
        deduplicated_stations l
    LEFT JOIN
        {{ ref('silver_nyc_borough_boundaries') }} bb
    ON
        ST_WITHIN(ST_GEOGPOINT(l.lon, l.lat), bb.boundary_polygon)
),

stations_with_data_quality_flags AS (
    SELECT
        t.*,
        -- flag for geographic issues (impossible lat/lng or null lat/lng)
        (t.lat IS NULL OR
        t.lon IS NULL OR
        (t.borough = 'Unknown' AND t.region_id IS NULL) OR
        {{ is_geographic_outlier('t.lat', 't.lon') }}) AS is_geography_quality_issue,

        -- flag for data integrity issues
        CASE WHEN
            t.name IS NULL OR
            t.station_id IS NULL OR
            t.short_name IS NULL
        THEN TRUE ELSE FALSE END AS is_data_integrity_issue,

    FROM stations_with_boroughs t
)

SELECT * FROM stations_with_data_quality_flags