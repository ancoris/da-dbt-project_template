{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'email_config_natural_key',
        cluster_by      = 'email_config_surrogate_key, email_config_natural_key, meta_start_time'
    )
}}
select
        e.email_config_surrogate_key,
        e.email_config_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.active,

        e.created_at_time,
        e.created_at_date,

        e.group_id,
        e.name,
        e.primary_role,
        e.product_id,
        e.reply_email,
        e.to_email,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_email_config_events') }} e
