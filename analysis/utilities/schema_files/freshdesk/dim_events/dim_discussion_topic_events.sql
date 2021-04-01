{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',
        '_fivetran_deleted',

        '_fivetran_synced_time',
        '_fivetran_synced_date',


        'created_at_time',
        'created_at_date',

        'discussion_forum_id',
        'hits',
        'locked',
        'merged_topic_id',
        'posts_count',
        'published',

        'replied_at_time',
        'replied_at_date',

        'replied_by',
        'stamp_type',
        'sticky',
        'title',

        'updated_at_time',
        'updated_at_date',

        'user_id',
        'user_votes',
        ],
        natural_key_col = 'discussion_topic_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'discussion_topic_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as discussion_topic_natural_key,  /* CHECK THIS */
        c.id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,


        c.created_at_time,
        c.created_at_date,

        c.discussion_forum_id,
        c.hits,
        c.locked,
        c.merged_topic_id,
        c.posts_count,
        c.published,

        c.replied_at_time,
        c.replied_at_date,

        c.replied_by,
        c.stamp_type,
        c.sticky,
        c.title,

        c.updated_at_time,
        c.updated_at_date,

        c.user_id,
        c.user_votes,
        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('discussion_topic_clean') }} c
where c.meta_is_valid = 1