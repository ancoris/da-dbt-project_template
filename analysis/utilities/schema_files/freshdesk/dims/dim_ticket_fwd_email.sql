{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'ticket_fwd_email_natural_key',
        cluster_by      = 'ticket_fwd_email_surrogate_key, ticket_fwd_email_natural_key, meta_start_time'
    )
}}
select
        e.ticket_fwd_email_surrogate_key,
        e.ticket_fwd_email_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.email,
        e.ticket_id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_ticket_fwd_email_events') }} e
