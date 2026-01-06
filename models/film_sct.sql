{% set env = target.name %} 
{% set schema_name = var('schema_name', 'sakila') %}
{% set table_name = var('table_name', 'film') %}

{{ log('📢 Running in environment: ' ~ env ~ ', Using schema: ' ~ schema_name ~ ', Table: ' ~ table_name, info=True) }}

SELECT 
    *
FROM {{ schema_name }}.{{ table_name }}
