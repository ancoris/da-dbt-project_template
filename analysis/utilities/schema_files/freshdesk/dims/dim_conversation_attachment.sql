{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'conversation_attachment_natural_key',
        cluster_by      = 'conversation_attachment_surrogate_key, conversation_attachment_natural_key, meta_start_time'
    )
}}
select
        e.conversation_attachment_surrogate_key,
        e.conversation_attachment_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.conversation_id,
        e.id,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.attachment_url,
        e.content_type,

        e.created_at_time,
        e.created_at_date,

        e.name,
        e.size,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_conversation_attachment_events') }} e
