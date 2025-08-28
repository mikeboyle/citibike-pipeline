{{ config(
    materialized='table',
    description='Distinct stations in the 90-day commute network'
) }}

WITH in_degree AS (
  SELECT 
    end_station_id AS station_id,
    COUNT(*) AS in_degree,
    SUM(num_trips) AS weighted_in_degree
  FROM {{ ref('gold_commuter_edges') }}
  GROUP BY end_station_id
),

out_degree AS (
  SELECT 
    start_station_id AS station_id,
    COUNT(*) AS out_degree,
    SUM(num_trips) AS weighted_out_degree
  FROM {{ ref('gold_commuter_edges') }}
  GROUP BY start_station_id
)

SELECT
  COALESCE(ind.station_id, outd.station_id) AS station_id,
  s.lat,
  s.lon,
  s.name,
  s.borough,
  s.capacity,
  COALESCE(ind.in_degree, 0) AS in_degree,
  COALESCE(outd.out_degree, 0) AS out_degree,
  COALESCE(ind.in_degree, 0) + COALESCE(outd.out_degree, 0) AS total_degree,
  COALESCE(ind.weighted_in_degree, 0) AS weighted_in_degree,
  COALESCE(outd.weighted_out_degree, 0) AS weighted_out_degree,
  COALESCE(ind.weighted_in_degree, 0) + COALESCE(outd.weighted_out_degree, 0) AS weighted_total_degree
FROM
  in_degree ind
FULL OUTER JOIN
  out_degree outd
ON
  ind.station_id = outd.station_id
LEFT JOIN
  {{ ref('silver_stations') }} s
ON
  COALESCE(ind.station_id, outd.station_id) = s.short_name
ORDER BY
  weighted_total_degree DESC, s.name