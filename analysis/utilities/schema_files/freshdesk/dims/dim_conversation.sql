{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'conversation_natural_key',
        cluster_by      = 'conversation_surrogate_key, conversation_natural_key, meta_start_time'
    )
}}
select
        e.conversation_surrogate_key,
        e.conversation_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e.ticket_id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.body,
        e.body_text,
        e.contact_id,

        e.created_at_time,
        e.created_at_date,

        e.from_email,
        e.incoming,
        e.private,
        e.source,
        e.support_email,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_conversation_events') }} e
