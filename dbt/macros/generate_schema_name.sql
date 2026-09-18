{# Use the layer name directly as the ClickHouse database (staging / intermediate / marts). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
