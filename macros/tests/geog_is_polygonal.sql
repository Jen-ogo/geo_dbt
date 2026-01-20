{% test geog_is_polygonal(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not (
    st_aswkt({{ column_name }}) like 'POLYGON(%'
    or st_aswkt({{ column_name }}) like 'MULTIPOLYGON(%'
  )
{% endtest %}