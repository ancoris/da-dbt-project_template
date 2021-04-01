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
        s.created_at,
        s.discussion_forum_id,
        s.hits,
        s.locked,
        s.merged_topic_id,
        s.posts_count,
        s.published,
        s.replied_at,
        s.replied_by,
        s.stamp_type,
        s.sticky,
        s.title,
        s.updated_at,
        s.user_id,
        s.user_votes,

        -- meta
        {{meta_process_time()}}                      as meta_delivery_time,
        {{meta_process_time()}}                      as meta_process_time
from {{ source('freshdesk', 'discussion_topic') }} s
