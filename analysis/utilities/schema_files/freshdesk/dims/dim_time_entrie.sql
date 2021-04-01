{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'time_entrie_natural_key',
        cluster_by      = 'time_entrie_surrogate_key, time_entrie_natural_key, meta_start_time'
    )
}}
select
        e.time_entrie_surrogate_key,
        e.time_entrie_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.agent_id,
        e.billable,
        e.company_id,

        e.created_at_time,
        e.created_at_date,


        e.executed_at_time,
        e.executed_at_date,

        e.note,

        e.start__time,
        e.start__date,

        e.ticket_id,
        e.time_spent,
        e.timer_running,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_time_entrie_events') }} e
