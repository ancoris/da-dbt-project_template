{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = 'discussion_comment_natural_key',
        cluster_by      = 'discussion_comment_surrogate_key, discussion_comment_natural_key, meta_start_time'
    )
}}
select
        e.discussion_comment_surrogate_key,
        e.discussion_comment_natural_key,

        -- attributes
        -- CHECK - REMOVE THE NATURAL KEY E.G. E.ID
        e.id,
        e._fivetran_deleted,

        e._fivetran_synced_time,
        e._fivetran_synced_date,

        e.answer,
        e.body,
        e.body_text,
        e.contact_id,

        e.created_at_time,
        e.created_at_date,

        e.discussion_topic_id,
        e.published,
        e.spam,
        e.trash,

        e.updated_at_time,
        e.updated_at_date,


        -- meta
        e.meta_process_time,
        e.meta_delivery_time,
        e.meta_scd_action,
        e.meta_start_time,
from {{ ref('dim_discussion_comment_events') }} e
