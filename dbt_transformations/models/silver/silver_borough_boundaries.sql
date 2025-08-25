{{ config(
    materialized='table',
    cluster_by=['borough_code']
) }}

SELECT
    borough_code,
    borough_name,
    ST_GEOGFROMGEOJSON(JSON_EXTRACT(feature_geojson, '$.geometry')) AS boundary_polygon,
    _ingested_at
FROM
    {{ source('raw', 'nyc_borough_boundaries' )}}
WHERE
    _ingested_at = (
        SELECT MAX(_ingested_at)
        FROM {{ source('raw', 'nyc_borough_boundaries' )}}
    )
