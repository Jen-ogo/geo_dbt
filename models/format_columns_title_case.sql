{% macro format_columns_title_case(columns=[], titlecase_cols=[]) %}

{{ log("📢 Columns to process: " ~ columns | join(', '), info=True) }}
{{ log("📢 Columns to title-case: " ~ titlecase_cols | join(', '), info=True) }}

{%- for col in columns %}
    {%- if not loop.first %}, {% endif %}
    {%- if col in titlecase_cols %}
        CONCAT(
            UPPER(SUBSTRING({{ col }}, 1, 1)),
            LOWER(SUBSTRING({{ col }}, 2))
        ) AS formatted_{{ col | replace('.', '_') }}
    {%- else %}
        {{ col }}
    {%- endif %}
{%- endfor %}
{% endmacro %}
