{# 
  Check if coordinates are geographic outliers using NYC bounding box
  Fast detection of obviously corrupted data (GPS errors, system glitches)
  
  Args:
    lat_col: Column name containing latitude values
    lng_col: Column name containing longitude values
    
  Returns:
    Boolean - true if coordinates fall outside generous NYC bounding box
#}
{% macro is_geographic_outlier(lat_col, lng_col) %}
  {{ lat_col }} < 40.4 OR {{ lat_col }} > 41.0 OR 
  {{ lng_col }} < -74.3 OR {{ lng_col }} > -73.7
{% endmacro %}