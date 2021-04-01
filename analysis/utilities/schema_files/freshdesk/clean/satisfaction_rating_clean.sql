{{
    config(
        materialized    = 'view',
        schema          ='clean_freshdesk'
    )
}}

select
        r.id                                                        as id,

        r._fivetran_synced                                          as _fivetran_synced_time,
        cast(r._fivetran_synced as date)                            as _fivetran_synced_date,

        r.agent_id                                                  as agent_id,
        r.contact_id                                                as contact_id,

        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.feedback                                                  as feedback,
        r.group_id                                                  as group_id,
        r.survey_id                                                 as survey_id,
        r.ticket_id                                                 as ticket_id,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('satisfaction_rating_raw') }} r
