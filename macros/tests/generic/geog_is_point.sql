{% test geog_is_point(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and st_aswkt({{ column_name }}) not like 'POINT(%'
{% endtest %}