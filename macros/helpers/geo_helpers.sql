{# ---------------------------
  WKT -> GEOGRAPHY (strict + allow) + chosen
---------------------------- #}
{% macro wkt_to_geog(wkt_col) -%}
  coalesce(
    try_to_geography({{ wkt_col }}),
    try_to_geography({{ wkt_col }}, true)
  )
{%- endmacro %}

{% macro wkt_to_geog_strict(wkt_col) -%}
  try_to_geography({{ wkt_col }})
{%- endmacro %}

{% macro wkt_to_geog_allow(wkt_col) -%}
  try_to_geography({{ wkt_col }}, true)
{%- endmacro %}

{# ---------------------------
  GEOGRAPHY -> WKT
---------------------------- #}
{% macro geog_to_wkt(geog_col) -%}
  st_aswkt({{ geog_col }})
{%- endmacro %}

{# ---------------------------
  GEOGRAPHY area in m2 (Snowflake GEOGRAPHY -> meters²)
---------------------------- #}
{% macro area_m2(geog_col) -%}
  st_area({{ geog_col }})
{%- endmacro %}