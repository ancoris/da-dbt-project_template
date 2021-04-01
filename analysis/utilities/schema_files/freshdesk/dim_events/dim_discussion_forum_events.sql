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

        'description',
        'discussion_category_id',
        'forum_type',
        'forum_visibility',
        'name',
        'position',
        'posts_count',
        'topics_count',
        ],
        natural_key_col = 'discussion_forum_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'discussion_forum_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as discussion_forum_natural_key,  /* CHECK THIS */
        c.id,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        c.description,
        c.discussion_category_id,
        c.forum_type,
        c.forum_visibility,
        c.name,
        c.position,
        c.posts_count,
        c.topics_count,
        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('discussion_forum_clean') }} c
where c.meta_is_valid = 1