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