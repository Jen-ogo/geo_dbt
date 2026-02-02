{% macro safe_num(col) -%}
  try_to_number({{ col }})
{%- endmacro %}

{% macro nullif_neg9999(col) -%}
  nullif({{ col }}, -9999)
{%- endmacro %}

{% macro safe_divide(num_expr, den_expr) -%}
  ({{ num_expr }}) / nullif(({{ den_expr }}), 0)
{%- endmacro %}

{% macro clamp(x_expr, lo_expr, hi_expr) -%}
  least(({{ hi_expr }}), greatest(({{ lo_expr }}), ({{ x_expr }})))
{%- endmacro %}

{% macro clamp01(x_expr) -%}
  least(1.0, greatest(0.0, ({{ x_expr }})))
{%- endmacro %}