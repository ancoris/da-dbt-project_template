{% materialization scd2, default %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set unique_key = config.get('natural_key_col') %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy = dbt_macros.scd2_strategy(model, config) %}

  {% if not target_relation_exists %}

      {% set build_sql = dbt_macros.build_initial_scd2_table(strategy, model['injected_sql']) %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, build_sql) }}
      {% endcall %}

  {% else %}

      -- build the table to hold all of the interim `insert` and `update` commands
      {% set staging_table = dbt_macros.build_scd2_staging_table(strategy, sql, target_relation) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'INTERNAL_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'internal_change_type')
                                   | rejectattr('name', 'equalto', 'internal_unique_key')
                                   | rejectattr('name', 'equalto', 'INTERNAL_UNIQUE_KEY')
                                   %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% call statement('main') %}
          {{ dbt_macros.scd2_merge_sql(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_source_columns
             )
          }}
      {% endcall %}

  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro scd2_strategy(node, config) %} -- change to config.require
    {% set check_cols = config['check_cols'] %}
    {% set primary_key_col = config['natural_key_col'] %}
    {% set modified_time = config['modified_time'] %}
    {% set created_time = config['created_time'] if config['created_time'] else modified_time %}
    {% set deleted_valid_to =  run_started_at.isoformat() %}

    {% if not primary_key_col %}
        {% do exceptions.raise_compiler_error("No value provided for 'primary_key_col' in config.") %}
    {% endif %}

    {% if not modified_time %}
        {% do exceptions.raise_compiler_error("No value provided for 'modified_time' in config.") %}
    {% endif %}

    {% if not (check_cols is iterable and (check_cols | length) > 0) %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols) %}
    {% endif %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key_col, modified_time]) %}

    {% do return({
        "unique_key": primary_key_col,
        "modified_time": modified_time,
        "created_time": created_time,
        "deleted_valid_to": deleted_valid_to,
        "cols_to_check": check_cols,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}

{% macro build_initial_scd2_table(strategy, sql) %}

    select *,
        {{ strategy.scd_id }} as meta_scd_id,
        {{ strategy.created_time }} as meta_start_time,
        cast(null as timestamp) as meta_end_time,
        true as meta_is_latest
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro build_scd2_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}

    {% set inserts_select = dbt_macros.scd2_staging_table_inserts(strategy, sql, target_relation) %}

    {% set updates_select = dbt_macros.scd2_staging_table_updates(strategy, sql, target_relation) %}

    {% call statement('build_scd2_staging_relation_inserts') %}
        {{ create_table_as(True, tmp_relation, inserts_select) }}
    {% endcall %}

    {% call statement('build_scd2_staging_relation_updates') %}
        insert into {{ tmp_relation }} (internal_change_type, meta_scd_id, meta_end_time, meta_is_latest)
        select internal_change_type, meta_scd_id, meta_end_time, meta_is_latest from (
            {{ updates_select }}
        ) tmp_sbq;
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro row_change_detection_exprxxx(source_a, source_b, cols) %}
  {% set row_change_detection_exprxxx -%}
        {% for col in cols %}
            to_json_string({{ source_a }}.{{ col }}) != to_json_string({{ source_b }}.{{ col }})
            {%- if not loop.last %} or {% endif %}

        {% endfor %}
  {%- endset %}
  {% do return(row_change_detection_exprxxx) %}
{% endmacro %}

{% macro scd2_staging_table_inserts(strategy, source_sql, target_relation) -%}

    with scd2_latest as (
        select  *,
                {{ strategy.unique_key }} as internal_unique_key
        from    {{ target_relation }}
        where   meta_is_latest
    ),

    source_data as (
        select  *,
                {{ strategy.scd_id }} as meta_scd_id,
                {{ strategy.unique_key }} as internal_unique_key
        from    ({{ source_sql }}) sub
    ),

    insertions as (
        select  'insert' as internal_change_type,
                source_data.*,
                case
                  when scd2_latest.internal_unique_key is null then
                    source_data.{{ strategy.created_time }}
                  else
                    source_data.{{ strategy.modified_time }} end as meta_start_time,
                cast(null as timestamp) as meta_end_time,
                true as meta_is_latest
        from    source_data
        left outer join
                scd2_latest
        on      scd2_latest.internal_unique_key = source_data.internal_unique_key
        where   scd2_latest.internal_unique_key is null -- new entry
          or    ({{ dbt_macros.row_change_detection_exprxxx('source_data', 'scd2_latest', strategy.cols_to_check) }}) -- updated entry
          or    scd2_latest.meta_end_time is not null -- reinserted entry
    )

    select * from insertions

{%- endmacro %}

{% macro scd2_staging_table_updates(strategy, source_sql, target_relation) -%}

    with scd2_latest as (
        select  *,
                {{ strategy.unique_key }} as internal_unique_key
        from    {{ target_relation }}
        where   meta_is_latest
    ),

    source_data as (
        select  *,
                {{ strategy.scd_id }} as meta_scd_id,
                {{ strategy.unique_key }} as internal_unique_key,
                {{ strategy.modified_time }} as meta_start_time
        from (
            {{ source_sql }}
        ) sbq
    ),

    updates as (

        -- all the items in source that have changed
        select  'update' as internal_change_type,
                scd2_latest.meta_scd_id,
                source_data.meta_start_time as meta_end_time,
                false as meta_is_latest
        from    source_data
        inner join
                scd2_latest
        on      scd2_latest.internal_unique_key = source_data.internal_unique_key
        where   (({{ dbt_macros.row_change_detection_exprxxx('source_data', 'scd2_latest', strategy.cols_to_check) }}))
        union all
        -- all the ended items in scd2 that have been re-added
        select  'update' as internal_change_type,
                scd2_latest.meta_scd_id,
                scd2_latest.meta_end_time,
                false as meta_is_latest
        from    source_data
        inner join
                scd2_latest
        on      scd2_latest.internal_unique_key = source_data.internal_unique_key
        where   scd2_latest.meta_end_time is not null
            and scd2_latest.meta_is_latest = true
        union all
        -- all the items active in scd2 but not in the source anymore (i.e. deletions)
        select  'update' as internal_change_type,
                scd2_latest.meta_scd_id,
                timestamp('{{ strategy.deleted_valid_to }}') as meta_end_time,
                true as meta_is_latest
        from    scd2_latest
        left outer join
                source_data on scd2_latest.internal_unique_key = source_data.internal_unique_key
        where   source_data.internal_unique_key is null
            and meta_is_latest = true
    )

    select * from updates

{%- endmacro %}

{% macro scd2_merge_sql(target, source, insert_cols) -%}
  {{ adapter_macro('dbt_macros.scd2_merge_sql', target, source, insert_cols) }}
{%- endmacro %}

{% macro default__scd2_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as meta_INTERNAL_DEST
    using {{ source }} as META_INTERNAL_SOURCE
    on META_INTERNAL_SOURCE.meta_scd_id = meta_INTERNAL_DEST.meta_scd_id

    when matched
     and META_INTERNAL_SOURCE.internal_change_type = 'update'
        then update
        set meta_end_time = META_INTERNAL_SOURCE.meta_end_time,
        meta_is_latest = META_INTERNAL_SOURCE.meta_is_latest

    when not matched
     and META_INTERNAL_SOURCE.internal_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})
    ;
{% endmacro %}
