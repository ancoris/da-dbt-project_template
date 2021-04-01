{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'agent_group_natural_key',
        cluster_by      = 'agent_group_surrogate_key, agent_group_natural_key, meta_start_time'
    )
}}
select
        e.agent_group_surrogate_key,
        e.agent_group_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.agent_id,
        e.group_id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_agent_group_events') }} e
