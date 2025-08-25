{{ config(
    materialized='table',
    cluster_by=['borough'],
    description='Station performance metrics for Network Optimization Team dashboard'
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

station_activity AS (
    SELECT start_station_id as station_id, COUNT(*) as starts, 0 as ends
    FROM {{ ref('silver_trips') }}
    WHERE DATE(started_at) >= (SELECT start_date from date_range)
        AND NOT {{ is_geographic_outlier('start_lat', 'start_lng') }}
        AND NOT {{ is_geographic_outlier('end_lat', 'end_lng') }}
    GROUP BY 1
  
    UNION ALL
  
    SELECT end_station_id as station_id, 0 as starts, COUNT(*) as ends  
    FROM {{ ref('silver_trips') }}
    WHERE DATE(started_at) >= (SELECT start_date from date_range)
        AND NOT {{ is_geographic_outlier('start_lat', 'start_lng') }}
        AND NOT {{ is_geographic_outlier('end_lat', 'end_lng') }}
    GROUP BY 1
)

SELECT 
    s.name as station_name,
    s.borough,
    s.lat,
    s.lon,
    s.capacity,
    sa.station_id,
    SUM(sa.starts) as total_starts,
    SUM(sa.ends) as total_ends,
    SUM(sa.starts) + SUM(sa.ends) as total_activity,
    -- Performance metrics
    ROUND((SUM(sa.starts) + SUM(sa.ends)) / 90.0, 1) as avg_daily_activity,
    CASE 
        WHEN s.capacity > 0 THEN ROUND((SUM(sa.starts) + SUM(sa.ends)) / (90.0 * s.capacity), 2)
        ELSE NULL 
    END as activity_per_dock_per_day,
    -- Imbalance indicators  
    SUM(sa.starts) - SUM(sa.ends) as net_outflow,
    CASE 
        WHEN (SUM(sa.starts) + SUM(sa.ends)) > 0 
        THEN ROUND(ABS(SUM(sa.starts) - SUM(sa.ends)) / (SUM(sa.starts) + SUM(sa.ends)) * 100, 1)
        ELSE NULL 
    END as imbalance_pct
FROM station_activity sa
JOIN {{ ref('silver_stations') }} s ON sa.station_id = s.short_name
GROUP BY s.name, s.borough, s.lat, s.lon, s.capacity, sa.station_id