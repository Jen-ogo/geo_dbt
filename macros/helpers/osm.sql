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