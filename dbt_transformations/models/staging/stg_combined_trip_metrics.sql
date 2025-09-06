{{ config(
    materialized='view'
) }}

WITH all_station_trip_metrics AS (
    -- Calculate performance metrics for ALL stations from trip data
    SELECT 
        station_id,
        COUNT(*) AS total_trips,
        AVG(CASE WHEN NOT is_temporal_outlier THEN trip_duration_seconds END) AS avg_trip_duration,
        MIN(DATE(station_event_time)) AS first_seen_in_trips,
        MAX(DATE(station_event_time)) AS last_seen_in_trips,
        DATE_DIFF(MAX(DATE(station_event_time)), MIN(DATE(station_event_time)), DAY) + 1 AS days_in_trip_data
    FROM (
        SELECT 
            start_station_id AS station_id,
            started_at AS station_event_time,
            is_temporal_outlier,
            trip_duration_seconds
        FROM {{ ref('silver_trips') }}
        UNION ALL
        SELECT 
            end_station_id AS station_id,
            ended_at AS station_event_time,
            is_temporal_outlier,
            trip_duration_seconds
        FROM {{ ref('silver_trips') }}
    )
    WHERE station_id IS NOT NULL 
        AND station_id != ''
    GROUP BY station_id
),

-- Metrics for trips that start at each station_id
start_station_metrics AS (
    SELECT 
        start_station_id AS station_id,
        COUNT(*) AS total_trips_started,
        AVG(CASE WHEN NOT is_temporal_outlier THEN trip_duration_seconds END) AS avg_trip_duration_started
    FROM {{ ref('silver_trips') }}
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
),

-- Metrics for trips that end at each station_id
end_station_metrics AS (
    SELECT 
        end_station_id AS station_id,
        COUNT(*) AS total_trips_ended,
        AVG(CASE WHEN NOT is_temporal_outlier THEN trip_duration_seconds END) AS avg_trip_duration_ended
    FROM {{ ref('silver_trips') }}
    WHERE end_station_id IS NOT NULL
    GROUP BY end_station_id
)

-- Combine all metrics
SELECT
    base.station_id,
    base.total_trips,
    base.avg_trip_duration,
    base.first_seen_in_trips,
    base.last_seen_in_trips,
    base.days_in_trip_data,
    COALESCE(start_m.total_trips_started, 0) AS total_trips_started,
    COALESCE(end_m.total_trips_ended, 0) AS total_trips_ended,
    start_m.avg_trip_duration_started,
    end_m.avg_trip_duration_ended,
    
    -- Efficiency metrics
    CASE 
        WHEN base.days_in_trip_data > 0 THEN COALESCE(start_m.total_trips_started, 0) / base.days_in_trip_data 
        ELSE NULL 
    END AS avg_daily_starts,
    CASE 
        WHEN base.days_in_trip_data > 0 THEN COALESCE(end_m.total_trips_ended, 0) / base.days_in_trip_data 
        ELSE NULL 
    END AS avg_daily_ends,
    CASE 
        WHEN base.days_in_trip_data > 0 THEN (COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0)) / base.days_in_trip_data 
        ELSE NULL 
    END AS avg_daily_activity,
    
    -- Balance metrics
    COALESCE(end_m.total_trips_ended, 0) - COALESCE(start_m.total_trips_started, 0) AS net_flow,
    CASE 
        WHEN (COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0)) > 0 
        THEN (COALESCE(end_m.total_trips_ended, 0) - COALESCE(start_m.total_trips_started, 0)) / 
                (COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0))
        ELSE NULL 
    END AS imbalance_ratio
    
FROM all_station_trip_metrics base
LEFT JOIN start_station_metrics start_m ON base.station_id = start_m.station_id
LEFT JOIN end_station_metrics end_m ON base.station_id = end_m.station_id