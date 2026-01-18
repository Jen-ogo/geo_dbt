{% macro safe_num(col) -%}
    try_to_number({{ col }})
{%- endmacro %}

{% macro nullif_neg9999(col) -%}
    nullif({{ col }}, -9999)
{%- endmacro %}