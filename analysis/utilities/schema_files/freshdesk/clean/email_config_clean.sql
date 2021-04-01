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

        r.active                                                    as active,

        r.created_at                                                as created_at_time,
        cast(r.created_at as date)                                  as created_at_date,

        r.group_id                                                  as group_id,
        r.name                                                      as name,
        r.primary_role                                              as primary_role,
        r.product_id                                                as product_id,
        r.reply_email                                               as reply_email,
        r.to_email                                                  as to_email,

        r.updated_at                                                as updated_at_time,
        cast(r.updated_at as date)                                  as updated_at_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('email_config_raw') }} r
