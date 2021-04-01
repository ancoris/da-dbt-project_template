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
        s.description,
        s.discussion_category_id,
        s.forum_type,
        s.forum_visibility,
        s.name,
        s.position,
        s.posts_count,
        s.topics_count,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'discussion_forum') }} s
