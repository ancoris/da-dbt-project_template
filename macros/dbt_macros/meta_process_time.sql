{% macro meta_process_time() %}
    timestamp('{{ var("replay_process_time", run_started_at.isoformat()) }}')
{% endmacro %}
