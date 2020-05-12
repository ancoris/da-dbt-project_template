{% macro meta_process_date() %}
    date('{{ var("replay_process_time", run_started_at.isoformat()) }}')
{% endmacro %}