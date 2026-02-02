{% macro nullif_empty_str(col) -%}
  nullif(trim({{ col }}), '')
{%- endmacro %}