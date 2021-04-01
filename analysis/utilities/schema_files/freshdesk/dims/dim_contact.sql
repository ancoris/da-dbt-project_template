{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'contact_natural_key',
        cluster_by      = 'contact_surrogate_key, contact_natural_key, meta_start_time'
    )
}}
select
        e.contact_surrogate_key,
        e.contact_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.active,
        e.address,
        e.company_id,

        e.created_at_time,
        e.created_at_date,

        e.description,
        e.email,
        e.job_title,
        e.language,
        e.mobile,
        e.name,
        e.phone,
        e.time_zone,
        e.twitter_id,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_contact_events') }} e
