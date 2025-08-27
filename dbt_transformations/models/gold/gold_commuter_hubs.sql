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
  ind.station_id,
  s.name,
  s.borough,
  s.capacity,
  ind.in_degree,
  outd.out_degree,
  ind.in_degree + outd.out_degree AS total_degree,
  ind.weighted_in_degree,
  outd.weighted_out_degree,
  ind.weighted_in_degree + outd.weighted_out_degree AS weighted_total_degree
FROM
  in_degree ind
JOIN
  out_degree outd
ON
  ind.station_id = outd.station_id
LEFT JOIN
  {{ ref('silver_stations') }} s
ON
  ind.station_id = s.short_name
ORDER BY
  weighted_total_degree DESC, s.name