{{ config(
    materialized='table',
    description='Distinct station-to-station pairs during commuting times in last 90 days'
) }}

WITH latest_data_date AS (
    SELECT MAX(DATE(started_at)) as max_date
    FROM {{ ref('silver_trips') }}
),

date_range AS (
    SELECT 
        max_date,
        DATE_SUB(DATE_TRUNC(max_date, MONTH), INTERVAL 2 MONTH) as start_date
    FROM latest_data_date
),

commuter_edges AS (
    SELECT
        start_station_id,
        end_station_id,
        COUNT(*) AS num_trips,
        AVG(trip_duration_seconds) AS avg_duration
    FROM {{ ref('silver_trips') }}
    WHERE DATE(started_at) >= (SELECT start_date from date_range)
        AND start_station_id <> end_station_id -- no self loops
        AND start_hour >= 7
        AND start_hour <= 10
        AND NOT is_weekend
        AND trip_duration_seconds > 60
    GROUP BY start_station_id, end_station_id
),

commuter_edges_enriched AS (
SELECT 
    e.start_station_id,
    s_start.name AS start_station_name,
    s_start.borough AS start_borough,
    s_start.lat AS start_lat,
    s_start.lon AS start_lon,
    e.end_station_id,
    s_end.name AS end_station_name,
    s_end.borough AS end_borough,
    s_end.lat AS end_lat,
    s_end.lon AS end_lon,
    e.num_trips,
    ST_DISTANCE(ST_GEOGPOINT(s_start.lon, s_start.lat), ST_GEOGPOINT(s_end.lon, s_end.lat)) AS distance_meters
FROM commuter_edges e
LEFT JOIN
    {{ ref('silver_stations') }} s_start
ON
    e.start_station_id = s_start.short_name
LEFT JOIN
    {{ ref('silver_stations') }} s_end
ON
    e.end_station_id = s_end.short_name
)

SELECT * 
FROM commuter_edges_enriched
WHERE
    start_borough IS NOT NULL
    AND end_borough IS NOT NULL
    AND ((start_borough = 'Manhattan' AND num_trips > 100)
        OR (start_borough <> 'Manhattan' AND num_trips > 50)) --arbitrary thresholds
ORDER BY num_trips DESC