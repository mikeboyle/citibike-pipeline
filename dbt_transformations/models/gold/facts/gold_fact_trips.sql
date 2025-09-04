{{ config(
    materialized='incremental',
    unique_key='ride_id',
    partition_by={
        "field": "date_key",
        "data_type": "date"
    },
    cluster_by=['start_station_id']
) }}

WITH trip_base AS (
    SELECT
        -- Primary and foreign keys
        ride_id,
        DATE(started_at) AS date_key,
        start_station_id,
        end_station_id,

        -- Data integrity flags
        is_temporal_outlier,
        is_geography_quality_issue,
        is_data_integrity_issue,
        is_duplicate_ride,

        -- Trip characteristics
        started_at,
        start_hour,
        ended_at,
        EXTRACT(HOUR FROM ended_at) AS end_hour,
        start_station_name,
        start_borough,
        start_lat,
        start_lng,
        end_station_name,
        end_borough,
        end_lat,
        end_lng,
        trip_duration_seconds,
        member_casual,
        gender,
        bike_id,
        birth_year,
        rideable_type,
        is_round_trip,
        CASE
            WHEN is_geography_quality_issue = true THEN NULL
            ELSE ST_DISTANCE(ST_GEOGPOINT(start_lng, start_lat), ST_GEOGPOINT(end_lng, end_lat))
        END AS distance_meters,

        CASE WHEN
            start_borough IS NOT NULL AND
            start_borough <> 'Unknown' AND
            end_borough IS NOT NULL AND
            end_borough <> 'Unknown' AND
            start_borough <> end_borough
        THEN TRUE ELSE FALSE END AS is_inter_borough_trip,

        -- metadata
        _ingested_at,
        _batch_key

        FROM {{ ref('silver_trips') }}
        {% if is_incremental() %}
            WHERE _batch_key LIKE '{{ var("month_key") }}%'
        {% endif %}
),

station_events AS (
    -- Convert each trip into two events: one start, one end
    SELECT
        ride_id,
        date_key,
        start_station_id as station_id,
        started_at as event_time,
        -1 as bike_change,  -- bike leaves station (start)
        'start' as event_type
    FROM trip_base

    UNION ALL

    SELECT
        ride_id,
        date_key,
        end_station_id as station_id,
        ended_at as event_time,
        +1 as bike_change,  -- bike arrives at station (end)
        'end' as event_type
    FROM trip_base
),

events_with_running_balance AS (
    SELECT
        *,
        SUM(bike_change) OVER (
            PARTITION BY station_id, date_key
            ORDER BY event_time, ride_id  -- ride_id for tie-breaking
            ROWS UNBOUNDED PRECEDING
        ) as running_bike_balance
    FROM station_events
),

-- Join running balance back to original trips
trips_with_balance AS (
    SELECT
        t.*,
        -- Get running balance of start station (after bike leaves start station)
        start_events.running_bike_balance as start_station_running_bike_balance,
        -- Get running balance of ending station (after bike arrives at end station)
        end_events.running_bike_balance as end_station_running_bike_balance

    FROM trip_base t

    LEFT JOIN events_with_running_balance start_events
        ON t.ride_id = start_events.ride_id
        AND start_events.event_type = 'start'

    LEFT JOIN events_with_running_balance end_events
        ON t.ride_id = end_events.ride_id
        AND end_events.event_type = 'end'
)

SELECT * FROM trips_with_balance