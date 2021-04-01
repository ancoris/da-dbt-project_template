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

        r.auto_ticket_assign                                        as auto_ticket_assign,
        r.business_hour_id                                          as business_hour_id,

        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.description                                               as description,
        r.escalate_to                                               as escalate_to,
        r.name                                                      as name,
        r.unassigned_for                                            as unassigned_for,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('group_raw') }} r
