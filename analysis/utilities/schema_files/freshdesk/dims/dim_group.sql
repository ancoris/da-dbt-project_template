{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'group_natural_key',
        cluster_by      = 'group_surrogate_key, group_natural_key, meta_start_time'
    )
}}
select
        e.group_surrogate_key,
        e.group_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.auto_ticket_assign,
        e.business_hour_id,

        e.created_at_time,
        e.created_at_date,

        e.description,
        e.escalate_to,
        e.name,
        e.unassigned_for,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_group_events') }} e
