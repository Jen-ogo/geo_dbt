{# ---------------------------
  OSM other_tags -> JSON
---------------------------- #}
{% macro osm_tags_json(other_tags_col) -%}
    iff(
      {{ other_tags_col }} is null or trim({{ other_tags_col }}) = '',
      null,
      try_parse_json('{' || replace({{ other_tags_col }}, '"=>"', '":"') || '}')
    )
{%- endmacro %}


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
  Dedup helper (QUALIFY row_number)
  Example:
    {{ dedup_qualify(
       partition_by=['osm_id'],
       order_by=['load_ts desc','source_file desc']
    ) }}
---------------------------- #}
{% macro dedup_qualify(partition_by, order_by) -%}
qualify row_number() over (
  partition by {{ partition_by | join(', ') }}
  order by {{ order_by | join(', ') }}
) = 1
{%- endmacro %}


{# ---------------------------
  H3 helpers
---------------------------- #}
{% macro h3_r10_from_geog_point(geog_point_col) -%}
    h3_point_to_cell_string({{ geog_point_col }}, 10)
{%- endmacro %}

{% macro h3_r10_from_geog_centroid(geog_poly_col) -%}
    h3_point_to_cell_string(st_centroid({{ geog_poly_col }}), 10)
{%- endmacro %}