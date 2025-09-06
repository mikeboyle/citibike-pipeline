{{ config(
    materialized='table'
) }}

-- Reconstructed stations
WITH reconstructed_stations AS (
    SELECT 
        r.station_id,
        r.station_name,
        r.short_name,
        r.lat,
        r.lon,
        r.borough,
        r.capacity,
        r.region_id,
        r.station_type,
        r.is_active,
        r.data_source,
        -- Add trip metrics
        tm.total_trips,
        tm.avg_trip_duration,
        tm.first_seen_in_trips,
        tm.last_seen_in_trips,
        tm.days_in_trip_data,
        tm.total_trips_started,
        tm.total_trips_ended,
        tm.avg_trip_duration_started,
        tm.avg_trip_duration_ended,
        tm.avg_daily_starts,
        tm.avg_daily_ends,
        tm.avg_daily_activity,
        tm.net_flow,
        tm.imbalance_ratio,
        CAST(NULL AS FLOAT64) AS activity_per_dock_per_day
    FROM {{ ref('stg_reconstructed_station_metadata') }} r
    LEFT JOIN {{ ref('stg_combined_trip_metrics') }} tm 
        ON r.station_id = tm.station_id
),

-- GBFS stations (current and historical)
gbfs_stations AS (
    SELECT 
        s.station_id,
        s.name AS station_name,
        s.short_name,
        s.lat,
        s.lon,
        s.borough,
        s.capacity,
        s.region_id,
        s.station_type,
        s.is_active,
        'gbfs' AS data_source,
        -- Add trip metrics
        tm.total_trips,
        tm.avg_trip_duration,
        tm.first_seen_in_trips,
        tm.last_seen_in_trips,
        tm.days_in_trip_data,
        tm.total_trips_started,
        tm.total_trips_ended,
        tm.avg_trip_duration_started,
        tm.avg_trip_duration_ended,
        tm.avg_daily_starts,
        tm.avg_daily_ends,
        tm.avg_daily_activity,
        tm.net_flow,
        tm.imbalance_ratio,
        CASE WHEN s.capacity IS NOT NULL AND s.capacity > 0 AND tm.days_in_trip_data > 0 
            THEN tm.total_trips / (s.capacity * tm.days_in_trip_data)
        ELSE NULL END as activity_per_dock_per_day
    FROM {{ ref('silver_stations') }} s
    LEFT JOIN {{ ref('stg_combined_trip_metrics') }} tm 
        ON s.short_name = tm.station_id
)

-- Union all stations
SELECT * FROM gbfs_stations
UNION ALL 
SELECT * FROM reconstructed_stations
