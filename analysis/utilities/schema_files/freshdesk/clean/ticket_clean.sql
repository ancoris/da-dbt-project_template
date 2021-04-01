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

        r.association_type                                          as association_type,
        r.company_id                                                as company_id,

        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.deleted                                                   as deleted,
        r.description                                               as description,
        r.description_text                                          as description_text,

        r.due_by                                                    as due_by_time,
        cast(r.due_by as date)                                      as due_by_date,

        r.email_config_id                                           as email_config_id,

        r.fr_due_by                                                 as fr_due_by_time,
        cast(r.fr_due_by as date)                                   as fr_due_by_date,

        r.fr_escalated                                              as fr_escalated,
        r.group_id                                                  as group_id,
        r.is_escalated                                              as is_escalated,
        r.priority                                                  as priority,
        r.product_id                                                as product_id,
        r.requester_id                                              as requester_id,
        r.responder_id                                              as responder_id,
        r.source                                                    as source,
        r.spam                                                      as spam,

        r.stats_closed_at                                           as stats_closed_at_time,
        cast(r.stats_closed_at as date)                             as stats_closed_at_date,


        r.stats_first_responded_at                                  as stats_first_responded_at_time,
        cast(r.stats_first_responded_at as date)                    as stats_first_responded_at_date,


        r.stats_resolved_at                                         as stats_resolved_at_time,
        cast(r.stats_resolved_at as date)                           as stats_resolved_at_date,

        r.status                                                    as status,
        r.subject                                                   as subject,
        r.type                                                      as type,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('ticket_raw') }} r
