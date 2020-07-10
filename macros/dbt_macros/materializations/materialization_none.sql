------------------------------------------------------------------------------------------------------------------------
-- Author: BSL
-- This materialisation is used to maintain static tables such as a date dimension.
-- The materialisation will only refresh the target on initial creation and in full-refresh-mode.
-- No action is taken on an incremental run.
------------------------------------------------------------------------------------------------------------------------
{% materialization materialization_none, default -%}

  {% set full_refresh_mode = flags.FULL_REFRESH %}


  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}


  {% call statement("main") %}
      {{ sql }}
  {% endcall %}


  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
