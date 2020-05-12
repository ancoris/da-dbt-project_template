{% macro clean_int64(string) %}
    safe_cast(regexp_replace(ifnull({{string}},'0'), r'[^0-9.]', '') as int64)
{% endmacro %}