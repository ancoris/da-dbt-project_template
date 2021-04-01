{{
    config(
        materialized    = 'view',
        schema          ='clean_freshdesk'
    )
}}

select
        r.id                                                        as id,
        r._fivetran_deleted                                         as _fivetran_deleted,

        r._fivetran_synced                                          as _fivetran_synced_time,
        cast(r._fivetran_synced as date)                            as _fivetran_synced_date,


        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.discussion_forum_id                                       as discussion_forum_id,
        r.hits                                                      as hits,
        r.locked                                                    as locked,
        r.merged_topic_id                                           as merged_topic_id,
        r.posts_count                                               as posts_count,
        r.published                                                 as published,

        r.replied_at                                                as replied_at_time,
        cast(r.replied_at as date)                                  as replied_at_date,

        r.replied_by                                                as replied_by,
        r.stamp_type                                                as stamp_type,
        r.sticky                                                    as sticky,
        r.title                                                     as title,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,

        r.user_id                                                   as user_id,
        r.user_votes                                                as user_votes,

        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('discussion_topic_raw') }} r
