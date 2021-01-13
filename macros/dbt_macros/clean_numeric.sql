{% macro clean_numeric(string) %}
    safe_cast(regexp_replace(ifnull({{string}},'0'), r'[^0-9.-]', '') as numeric)
{% endmacro %}
