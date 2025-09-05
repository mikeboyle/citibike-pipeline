{{ config(
        materialized='table'
) }}

WITH date_spine AS (
    SELECT date_key
    FROM UNNEST(GENERATE_DATE_ARRAY('2013-05-27', '2026-12-31')) AS date_key
)

SELECT
    d.date_key,
    EXTRACT(YEAR FROM d.date_key) AS year,
    EXTRACT(MONTH FROM d.date_key) AS month,
    FORMAT_DATE('%B', d.date_key) AS month_name,
    EXTRACT(DAY FROM d.date_key) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM d.date_key) AS day_of_week,
    FORMAT_DATE('%A', d.date_key) AS day_name,
    EXTRACT(QUARTER FROM d.date_key) AS quarter,
    EXTRACT(WEEK FROM d.date_key) AS week_of_year,

    EXTRACT(DAYOFWEEK FROM d.date_key) IN (1, 7) AS is_weekend,    
    (h.holiday_date IS NOT NULL) AS is_holiday,
    h.holiday_name, -- nullable
    {{ derive_season('d.date_key') }} AS season

FROM date_spine d
LEFT JOIN {{ ref('holidays') }} h
ON d.date_key = h.holiday_date
ORDER BY date_key    
    