{{ config(
    materialized='view'
) }}

-- Identify stations that fell back to legacy IDs (missing from GBFS)
WITH missing_station_ids AS (
    (SELECT start_station_id AS station_id
    FROM {{ ref('silver_trips') }}
    WHERE start_station_id = legacy_start_station_id)

    UNION DISTINCT
    
    (SELECT end_station_id AS station_id
    FROM {{ ref('silver_trips') }}
    WHERE end_station_id = legacy_end_station_id)
),

-- Reconstruct metadata for missing stations, except borough
reconstructed_stations_without_boroughs AS (
    SELECT 
        m.station_id,
        ANY_VALUE(CASE WHEN t.start_station_id = m.station_id THEN t.start_station_name END) AS station_name,
        CAST(NULL AS STRING) AS short_name,
        AVG(CASE WHEN t.start_station_id = m.station_id AND NOT t.is_geography_quality_issue 
                THEN t.start_lat END) AS lat,
        AVG(CASE WHEN t.start_station_id = m.station_id AND NOT t.is_geography_quality_issue 
                THEN t.start_lng END) AS lon,
        NULL AS capacity,
        CAST(NULL AS STRING) AS region_id,
        CAST(NULL AS STRING) AS station_type,
        FALSE AS is_active,
        'reconstructed_from_trips' AS data_source
    FROM missing_station_ids m
    LEFT JOIN {{ ref('silver_trips') }} t 
        ON m.station_id IN (t.start_station_id, t.end_station_id)
    GROUP BY m.station_id
)

SELECT
    r.*,
    COALESCE(bb.borough_name, 'Unknown') AS borough
FROM
    reconstructed_stations_without_boroughs r
LEFT JOIN
    {{ ref('silver_nyc_borough_boundaries') }} bb
ON
    ST_WITHIN(ST_GEOGPOINT(r.lon, r.lat), bb.boundary_polygon)


