{# ---------------------------
  H3 index from GEOGRAPHY point / centroid
  Snowflake built-ins:
    - H3_POINT_TO_CELL_STRING(geog_point, res)
---------------------------- #}

{% macro h3_r10_from_geog_point(geog_point_col) -%}
  h3_point_to_cell_string({{ geog_point_col }}, 10)
{%- endmacro %}

{% macro h3_r10_from_geog_centroid(geog_poly_col) -%}
  h3_point_to_cell_string(st_centroid({{ geog_poly_col }}), 10)
{%- endmacro %}

{% macro h3_r7_from_geog_point(geog_point_col) -%}
  h3_point_to_cell_string({{ geog_point_col }}, 7)
{%- endmacro %}

{% macro h3_r7_from_geog_centroid(geog_poly_col) -%}
  h3_point_to_cell_string(st_centroid({{ geog_poly_col }}), 7)
{%- endmacro %}


{# ---------------------------
  Expected cell count in k-ring (same formula as you used)
  N = 1 + 3*k*(k+1)
---------------------------- #}
{% macro h3_expected_cells(k_expr) -%}
  (1 + 3 * ({{ k_expr }}) * (({{ k_expr }}) + 1))
{%- endmacro %}


{# ---------------------------
  Snowflake "kring" analog:
  Databricks: explode(h3_kring(...))
  Snowflake:  H3_GRID_DISK(h3_cell, k) -> ARRAY
             + LATERAL FLATTEN to rows
  IMPORTANT: returns ARRAY of same type as input (VARCHAR if input is VARCHAR)
---------------------------- #}

{% macro h3_grid_disk_array(h3_cell_expr, k_expr) -%}
  h3_grid_disk({{ h3_cell_expr }}, {{ k_expr }})
{%- endmacro %}

{# emits: lateral flatten(input => H3_GRID_DISK(...)) <alias> #}
{% macro h3_grid_disk_flatten(h3_cell_expr, k_expr, alias='f') -%}
  lateral flatten(input => h3_grid_disk({{ h3_cell_expr }}, {{ k_expr }})) {{ alias }}
{%- endmacro %}

{# value accessor for flattened rows #}
{% macro h3_flatten_value(alias='f') -%}
  {{ alias }}.value::string
{%- endmacro %}