{% macro generate_metadata(meta_produced_at_column=None) %}
    current_timestamp as meta_created_at,
    current_timestamp as meta_updated_at,
    {% if meta_produced_at_column %}{{ meta_produced_at_column }} as meta_produced_at
    {% else %}timestamp('{{ run_started_at.isoformat() }}') as meta_produced_at
    {% endif %}
{% endmacro %}