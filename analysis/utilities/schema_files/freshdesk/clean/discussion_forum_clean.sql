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

        r.description                                               as description,
        r.discussion_category_id                                    as discussion_category_id,
        r.forum_type                                                as forum_type,
        r.forum_visibility                                          as forum_visibility,
        r.name                                                      as name,
        r.position                                                  as position,
        r.posts_count                                               as posts_count,
        r.topics_count                                              as topics_count,

        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('discussion_forum_raw') }} r
