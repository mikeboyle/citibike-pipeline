{#
  Start date of the Citibike NYC program
#}
{% macro citibike_inception_date() %}
  '2013-05-27'
{% endmacro %}

{#
  Current datetime in NYC time zone (right now)
#}
{% macro now_nyc_datetime() %}
  CURRENT_DATETIME("America/New_York")
{% endmacro %}

{#
  Season for the given datetime column (NYC seasons)

  Args:
    date_col: Column name containing datetime or formatted YYYY-MM-DD date string

  Returns:
    String - Winter, Spring, Summer, or Fall
#}
{% macro derive_season(date_col) %}
  CASE
    WHEN EXTRACT(MONTH FROM {{ date_col }}) IN (12,1,2) THEN 'Winter'
    WHEN EXTRACT(MONTH FROM {{ date_col }}) IN (3,4,5) THEN 'Spring'
    WHEN EXTRACT(MONTH FROM {{ date_col }}) IN (6,7,8) THEN 'Summer'
    ELSE 'Fall'
  END
{% endmacro %}