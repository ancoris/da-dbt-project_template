{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'business_hour_natural_key',
        cluster_by      = 'business_hour_surrogate_key, business_hour_natural_key, meta_start_time'
    )
}}
select
        e.business_hour_surrogate_key,
        e.business_hour_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        e.created_at_time,
        e.created_at_date,

        e.description,
        e.name,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_business_hour_events') }} e
