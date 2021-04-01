{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'priority_lookup_natural_key',
        cluster_by      = 'priority_lookup_surrogate_key, priority_lookup_natural_key, meta_start_time'
    )
}}
select
        e.priority_lookup_surrogate_key,
        e.priority_lookup_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.priority,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_priority_lookup_events') }} e
