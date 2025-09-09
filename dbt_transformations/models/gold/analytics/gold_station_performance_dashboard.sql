{{ config(
    materialized='table',
    cluster_by=['borough'],
    description='Station performance metrics for Network Optimization Team dashboard'
) }}

WITH latest_data_date AS (
    SELECT MAX(DATE(started_at)) AS max_date
    FROM {{ ref('gold_fact_trips') }}
),

data_start_date AS (
    SELECT
        max_date,
        DATE_SUB(DATE_TRUNC(max_date, MONTH), INTERVAL 2 MONTH) AS start_date
    FROM
        latest_data_date
),

date_range AS (
    SELECT
        max_date,
        start_date, 
        DATE_DIFF(max_date, start_date, DAY) AS actual_days
    FROM data_start_date
),

recent_trips AS (
    SELECT * 
    FROM {{ ref('gold_fact_trips') }}
    WHERE date_key >= (SELECT start_date FROM date_range)
        AND NOT is_data_integrity_issue
        AND NOT is_geography_quality_issue
        AND NOT is_temporal_outlier
        AND NOT is_duplicate_ride
),

station_activity AS (
    SELECT 
        start_station_id AS station_id, 
        COUNT(*) AS starts, 
        0 AS ends,
    FROM recent_trips
    GROUP BY 1
    
    UNION ALL
    
    SELECT 
        end_station_id AS station_id, 
        0 AS starts, 
        COUNT(*) AS ends,
    FROM recent_trips
    GROUP BY 1
),

aggregated_activity AS (
    SELECT 
        station_id,
        SUM(starts) AS total_starts,
        SUM(ends) AS total_ends,
        SUM(starts) + SUM(ends) AS total_activity,
        SUM(ends) - SUM(starts) AS net_inflow,  -- Positive = gaining bikes, negative = losing bikes
    FROM station_activity
    GROUP BY station_id
),

daily_rush_hour_patterns AS (
    SELECT 
        rt.start_station_id AS station_id,
        rt.date_key,
        dd.day_of_week,
        -- Morning rush daily min/max per station
        COALESCE(
            MIN(CASE WHEN rt.start_hour BETWEEN 7 AND 10 THEN rt.start_station_running_bike_balance END),
            0
         ) AS daily_morning_min,
        COALESCE(
            MAX(CASE WHEN rt.start_hour BETWEEN 7 AND 10 THEN rt.start_station_running_bike_balance END),
            0
        ) AS daily_morning_max,
        -- Evening rush daily min/max per station  
        COALESCE(
            MIN(CASE WHEN rt.start_hour BETWEEN 17 AND 20 THEN rt.start_station_running_bike_balance END),
            0
        ) AS daily_evening_min,
        COALESCE(
            MAX(CASE WHEN rt.start_hour BETWEEN 17 AND 20 THEN rt.start_station_running_bike_balance END),
            0
        ) AS daily_evening_max
    FROM recent_trips rt
    JOIN {{ ref('gold_dim_dates') }} dd 
    ON rt.date_key = dd.date_key
    WHERE NOT dd.is_weekend
    GROUP BY rt.start_station_id, rt.date_key, dd.day_of_week
),

station_rush_hour_metrics AS (
    SELECT 
        station_id,
        -- Average of daily minimums/maximums
        AVG(daily_morning_min) AS avg_daily_morning_min,
        AVG(daily_morning_max) AS avg_daily_morning_max,
        AVG(daily_evening_min) AS avg_daily_evening_min,
        AVG(daily_evening_max) AS avg_daily_evening_max,
        -- Daily swing patterns
        AVG(daily_morning_max - daily_morning_min) AS avg_morning_daily_swing,
        AVG(daily_evening_max - daily_evening_min) AS avg_evening_daily_swing
    FROM daily_rush_hour_patterns
    WHERE daily_morning_min IS NOT NULL OR daily_evening_min IS NOT NULL
    GROUP BY station_id
)

SELECT 
    ds.station_id,
    ds.station_name,
    ds.borough,
    ds.lat,
    ds.lon,
    ds.capacity,
    ds.is_active,
    
    -- Recent activity metrics (last ~90 days)
    COALESCE(aa.total_starts, 0) AS recent_starts,
    COALESCE(aa.total_ends, 0) AS recent_ends,
    COALESCE(aa.total_activity, 0) AS recent_activity,
    COALESCE(aa.net_inflow, 0) AS recent_net_inflow,  -- Positive = gaining bikes
    
    -- Performance metrics
    CASE 
        WHEN (SELECT actual_days FROM date_range) > 0 
        THEN ROUND(COALESCE(aa.total_activity, 0) / (SELECT actual_days FROM date_range), 1)
        ELSE NULL 
    END AS avg_daily_activity,
    CASE 
        WHEN ds.capacity > 0 AND (SELECT actual_days FROM date_range) > 0
        THEN ROUND(COALESCE(aa.total_activity, 0) / ((SELECT actual_days FROM date_range) * ds.capacity), 2)
        ELSE NULL 
    END AS activity_per_dock_per_day,
    
    -- Imbalance metrics
    CASE 
        WHEN COALESCE(aa.total_activity, 0) > 0 
        THEN ROUND(COALESCE(aa.net_inflow, 0) / aa.total_activity, 2)
        ELSE NULL 
    END AS imbalance_ratio,  -- Positive = station gaining bikes
    
    -- Rush hour operational patterns (weekdays only)
    ROUND(srh.avg_daily_morning_min, 1) AS avg_daily_morning_min_bike_balance,
    ROUND(srh.avg_daily_morning_max, 1) AS avg_daily_morning_max_bike_balance,
    ROUND(srh.avg_daily_evening_min, 1) AS avg_daily_evening_min_bike_balance,
    ROUND(srh.avg_daily_evening_max, 1) AS avg_daily_evening_max_bike_balance,
    ROUND(srh.avg_morning_daily_swing, 1) AS avg_morning_daily_bike_balance_swing,
    ROUND(srh.avg_evening_daily_swing, 1) AS avg_evening_daily_bike_balance_swing,

    CASE
        WHEN COALESCE(ds.capacity, 0) > 0
        THEN ROUND((srh.avg_morning_daily_swing / capacity), 2)
        ELSE NULL
    END AS balance_swing_ratio_morning,

    CASE
        WHEN COALESCE(ds.capacity, 0) > 0
        THEN ROUND((srh.avg_evening_daily_swing / capacity), 2)
        ELSE NULL
    END AS balance_swing_ratio_evening,

FROM 
    {{ ref('gold_dim_stations') }} ds
INNER JOIN 
    aggregated_activity aa
ON
    ds.short_name = aa.station_id
LEFT JOIN 
    station_rush_hour_metrics srh
ON
    ds.short_name = srh.station_id