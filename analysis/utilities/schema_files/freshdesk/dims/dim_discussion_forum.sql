{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'discussion_forum_natural_key',
        cluster_by      = 'discussion_forum_surrogate_key, discussion_forum_natural_key, meta_start_time'
    )
}}
select
        e.discussion_forum_surrogate_key,
        e.discussion_forum_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.description,
        e.discussion_category_id,
        e.forum_type,
        e.forum_visibility,
        e.name,
        e.position,
        e.posts_count,
        e.topics_count,

        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_discussion_forum_events') }} e
