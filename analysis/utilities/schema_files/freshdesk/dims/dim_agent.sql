{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'agent_natural_key',
        cluster_by      = 'agent_surrogate_key, agent_natural_key, meta_start_time'
    )
}}
select
        e.agent_surrogate_key,
        e.agent_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.available,

        e.available_since_time,
        e.available_since_date,

        e.contact_active,

        e.contact_created_at_time,
        e.contact_created_at_date,

        e.contact_email,
        e.contact_job_title,
        e.contact_language,

        e.contact_last_login_at_time,
        e.contact_last_login_at_date,

        e.contact_mobile,
        e.contact_name,
        e.contact_phone,
        e.contact_time_zone,

        e.contact_updated_at_time,
        e.contact_updated_at_date,


        e.created_at_time,
        e.created_at_date,

        e.occasional,
        e.signature,
        e.ticket_scope,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_agent_events') }} e
