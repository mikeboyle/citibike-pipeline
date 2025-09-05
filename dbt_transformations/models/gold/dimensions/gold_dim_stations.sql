{{ config(
    materialized='table'
) }}

WITH all_station_trip_metrics AS (
    -- Calculate performance metrics for ALL stations from trip data
    SELECT 
        station_id,
        COUNT(*) as total_trips,
        MIN(DATE(station_event_time)) as first_seen_in_trips,
        MAX(DATE(station_event_time)) as last_seen_in_trips,
        DATE_DIFF(MAX(DATE(station_event_time)), MIN(DATE(station_event_time)), DAY) + 1 as days_in_trip_data
    FROM (
        SELECT start_station_id as station_id, started_at as station_event_time FROM {{ ref('silver_trips') }}
        UNION ALL
        SELECT end_station_id as station_id, ended_at as station_event_time FROM {{ ref('silver_trips') }}
    )
    WHERE station_id IS NOT NULL 
        AND station_id != ''
    GROUP BY station_id
),

start_station_metrics AS (
    SELECT 
        start_station_id as station_id,
        COUNT(*) as total_trips_started,
        AVG(trip_duration_seconds) as avg_trip_duration_started
    FROM {{ ref('silver_trips') }}
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
),

end_station_metrics AS (
    SELECT 
        end_station_id as station_id,
        COUNT(*) as total_trips_ended,
        AVG(trip_duration_seconds) as avg_trip_duration_ended
    FROM {{ ref('silver_trips') }}
    WHERE end_station_id IS NOT NULL
    GROUP BY end_station_id
),

combined_trip_metrics AS (
    SELECT 
        base.station_id,
        base.total_trips,
        base.first_seen_in_trips,
        base.last_seen_in_trips,
        base.days_in_trip_data,
        COALESCE(start_m.total_trips_started, 0) as total_trips_started,
        COALESCE(end_m.total_trips_ended, 0) as total_trips_ended,
        COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0) as total_activity,
        start_m.avg_trip_duration_started,
        end_m.avg_trip_duration_ended,
        
        -- Efficiency metrics
        CASE 
            WHEN base.days_in_trip_data > 0 THEN COALESCE(start_m.total_trips_started, 0) / base.days_in_trip_data 
            ELSE NULL 
        END as avg_daily_starts,
        CASE 
            WHEN base.days_in_trip_data > 0 THEN COALESCE(end_m.total_trips_ended, 0) / base.days_in_trip_data 
            ELSE NULL 
        END as avg_daily_ends,
        CASE 
            WHEN base.days_in_trip_data > 0 THEN (COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0)) / base.days_in_trip_data 
            ELSE NULL 
        END as avg_daily_activity,
        
        -- Balance metrics
        COALESCE(start_m.total_trips_started, 0) - COALESCE(end_m.total_trips_ended, 0) as net_flow,
        CASE 
            WHEN (COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0)) > 0 
            THEN (COALESCE(start_m.total_trips_started, 0) - COALESCE(end_m.total_trips_ended, 0)) / 
                 (COALESCE(start_m.total_trips_started, 0) + COALESCE(end_m.total_trips_ended, 0))
            ELSE NULL 
        END as imbalance_ratio
        
    FROM all_station_trip_metrics base
    LEFT JOIN start_station_metrics start_m ON base.station_id = start_m.station_id
    LEFT JOIN end_station_metrics end_m ON base.station_id = end_m.station_id
),

-- Identify stations that fell back to legacy IDs (missing from GBFS)
missing_station_ids AS (
    (SELECT start_station_id as station_id
    FROM {{ ref('silver_trips') }}
    WHERE start_station_id = legacy_start_station_id)

    UNION DISTINCT
    
    (SELECT end_station_id as station_id
    FROM {{ ref('silver_trips') }}
    WHERE end_station_id = legacy_end_station_id)
),

-- Reconstruct metadata for missing stations
reconstructed_station_metadata AS (
    SELECT 
        m.station_id,
        ANY_VALUE(CASE WHEN t.start_station_id = m.station_id THEN t.start_station_name END) as station_name,
        CAST(NULL AS STRING) as short_name,
        AVG(CASE WHEN t.start_station_id = m.station_id AND NOT t.is_geography_quality_issue 
             THEN t.start_lat END) as lat,
        AVG(CASE WHEN t.start_station_id = m.station_id AND NOT t.is_geography_quality_issue 
             THEN t.start_lng END) as lon,
        ANY_VALUE(CASE WHEN t.start_station_id = m.station_id THEN t.start_borough END) as borough,
        NULL as capacity,
        CAST(NULL AS STRING) as region_id,
        CAST(NULL AS STRING) as station_type,
        FALSE as is_active,
        'reconstructed_from_trips' as data_source
    FROM missing_station_ids m
    LEFT JOIN {{ ref('silver_trips') }} t 
        ON m.station_id IN (t.start_station_id, t.end_station_id)
    GROUP BY m.station_id
),

-- Reconstructed stations
reconstructed_stations AS (
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
        tm.first_seen_in_trips,
        tm.last_seen_in_trips,
        tm.days_in_trip_data,
        tm.total_trips_started,
        tm.total_trips_ended,
        tm.total_activity,
        tm.avg_trip_duration_started,
        tm.avg_trip_duration_ended,
        tm.avg_daily_starts,
        tm.avg_daily_ends,
        tm.avg_daily_activity,
        tm.net_flow,
        tm.imbalance_ratio
    FROM reconstructed_station_metadata r
    LEFT JOIN combined_trip_metrics tm ON r.station_id = tm.station_id
),

-- GBFS stations (current and historical)
gbfs_stations AS (
    SELECT 
        s.station_id,
        s.name as station_name,
        s.short_name,
        s.lat,
        s.lon,
        s.borough,
        s.capacity,
        s.region_id,
        s.station_type,
        s.is_active,
        'gbfs' as data_source,
        -- Add trip metrics
        tm.total_trips,
        tm.first_seen_in_trips,
        tm.last_seen_in_trips,
        tm.days_in_trip_data,
        tm.total_trips_started,
        tm.total_trips_ended,
        tm.total_activity,
        tm.avg_trip_duration_started,
        tm.avg_trip_duration_ended,
        tm.avg_daily_starts,
        tm.avg_daily_ends,
        tm.avg_daily_activity,
        tm.net_flow,
        tm.imbalance_ratio
    FROM {{ ref('silver_stations') }} s
    LEFT JOIN combined_trip_metrics tm ON s.short_name = tm.station_id
),

-- Union all stations
final_stations AS (
    SELECT * FROM gbfs_stations
    UNION ALL 
    SELECT * FROM reconstructed_stations
)

SELECT 
    *,
    CASE WHEN capacity IS NOT NULL AND capacity > 0 AND days_in_trip_data > 0 
         THEN total_activity / (capacity * days_in_trip_data)
         ELSE NULL END as activity_per_dock_per_day
FROM final_stations