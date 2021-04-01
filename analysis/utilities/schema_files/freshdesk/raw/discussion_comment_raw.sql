{{
    config(
        materialized    = 'view',
        schema          = 'raw_freshdesk'
    )
}}
select
        s.id,
        s._fivetran_deleted,
        s._fivetran_synced,
        s.answer,
        s.body,
        s.body_text,
        s.contact_id,
        s.created_at,
        s.discussion_topic_id,
        s.published,
        s.spam,
        s.trash,
        s.updated_at,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'discussion_comment') }} s
