{% macro normalize_station_id(station_id_field) %}
  CASE 
    WHEN REGEXP_CONTAINS({{ station_id_field }}, r'^\d+\.\d+$')
    THEN FORMAT("%.2f", CAST({{ station_id_field }} AS FLOAT64))
    ELSE {{ station_id_field }}
  END
{% endmacro %}