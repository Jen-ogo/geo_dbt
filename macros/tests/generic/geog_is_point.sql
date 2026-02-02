{% test geog_is_point(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null
   or not upper(st_aswkt({{ column_name }})) like 'POINT%'

{% endtest %}
