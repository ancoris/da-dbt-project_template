{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'discussion_topic_natural_key',
        cluster_by      = 'discussion_topic_surrogate_key, discussion_topic_natural_key, meta_start_time'
    )
}}
select
        e.discussion_topic_surrogate_key,
        e.discussion_topic_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,


        e.created_at_time,
        e.created_at_date,

        e.discussion_forum_id,
        e.hits,
        e.locked,
        e.merged_topic_id,
        e.posts_count,
        e.published,

        e.replied_at_time,
        e.replied_at_date,

        e.replied_by,
        e.stamp_type,
        e.sticky,
        e.title,

        e.updated_at_time,
        e.updated_at_date,

        e.user_id,
        e.user_votes,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_discussion_topic_events') }} e
