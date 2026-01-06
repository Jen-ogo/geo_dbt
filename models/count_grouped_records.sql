{% macro count_grouped_records(table_name, count_column, group_by_column) %}
SELECT 
    {{ group_by_column }},
    COUNT({{ count_column }}) AS total_records
FROM {{ table_name }}
GROUP BY {{ group_by_column }}
{% endmacro %}

