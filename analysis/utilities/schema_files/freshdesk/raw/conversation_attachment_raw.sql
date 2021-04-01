{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.conversation_id,
        s.id,
        s._fivetran_synced,
        s.attachment_url,
        s.content_type,
        s.created_at,
        s.name,
        s.size,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'conversation_attachment') }} s
