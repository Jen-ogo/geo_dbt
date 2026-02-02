{% test is_h3_hex(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null
   or not h3_is_valid_cell({{ column_name }})

{% endtest %}
