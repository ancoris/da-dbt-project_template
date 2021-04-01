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

        r.answer                                                    as answer,
        r.body                                                      as body,
        r.body_text                                                 as body_text,
        r.contact_id                                                as contact_id,

        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.discussion_topic_id                                       as discussion_topic_id,
        r.published                                                 as published,
        r.spam                                                      as spam,
        r.trash                                                     as trash,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('discussion_comment_raw') }} r
