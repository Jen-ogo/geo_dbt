{% test wkt_prefix_any(model, column_name, prefixes) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and not (
    {% for p in prefixes -%}
      upper({{ column_name }}) like '{{ p | upper }}%'
      {%- if not loop.last %} or {% endif -%}
    {%- endfor %}
  )

{% endtest %}
