{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',
        '_fivetran_deleted',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        'agent_id',
        'billable',
        'company_id',

        'created_at_time',
        'created_at_date',


        'executed_at_time',
        'executed_at_date',

        'note',

        'start__time',
        'start__date',

        'ticket_id',
        'time_spent',
        'timer_running',

        'updated_at_time',
        'updated_at_date',

        ],
        natural_key_col = 'time_entrie_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'time_entrie_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as time_entrie_natural_key,  /* CHECK THIS */
        c.id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.agent_id,
        c.billable,
        c.company_id,

        c.created_at_time,
        c.created_at_date,


        c.executed_at_time,
        c.executed_at_date,

        c.note,

        c.start__time,
        c.start__date,

        c.ticket_id,
        c.time_spent,
        c.timer_running,

        c.updated_at_time,
        c.updated_at_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('time_entries_clean') }} c
where c.meta_is_valid = 1