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
    FROM {{ source('raw', 'citibike_stations') }}

)

SELECT *
FROM parsed_stations
QUALIFY 
    ROW_NUMBER() OVER (
        PARTITION BY station_id
        ORDER BY _ingested_at DESC
    ) = 1