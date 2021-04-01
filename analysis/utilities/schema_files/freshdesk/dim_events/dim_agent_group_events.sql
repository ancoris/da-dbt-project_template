{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'agent_id',
        'group_id',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        ],
        natural_key_col = 'agent_group_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'agent_group_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.agent_id          as agent_group_natural_key,  /* CHECK THIS */
        c.agent_id,
        c.group_id,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('agent_group_clean') }} c
where c.meta_is_valid = 1