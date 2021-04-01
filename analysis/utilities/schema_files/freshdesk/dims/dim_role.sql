{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'role_natural_key',
        cluster_by      = 'role_surrogate_key, role_natural_key, meta_start_time'
    )
}}
select
        e.role_surrogate_key,
        e.role_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        e.created_at_time,
        e.created_at_date,

        e.default,
        e.description,
        e.name,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_role_events') }} e
