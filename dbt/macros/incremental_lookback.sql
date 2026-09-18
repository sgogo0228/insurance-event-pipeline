{# Read only new raw rows, re-reading a lookback window for late inserts; re-read rows are replaced via unique_key. #}
{% macro incremental_lookback(column='ingested_at', minutes=10) -%}
    {% if is_incremental() %}
      and {{ column }} >= (select max({{ column }}) from {{ this }}) - interval {{ minutes }} minute
    {% endif %}
{%- endmacro %}
