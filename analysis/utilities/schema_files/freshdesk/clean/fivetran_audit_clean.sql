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


        r.done                                                      as done_time,
        cast(r.done as date)                                        as done_date,

        r.message                                                   as message,

        r.progress                                                  as progress_time,
        cast(r.progress as date)                                    as progress_date,

        r.rows_updated_or_inserted                                  as rows_updated_or_inserted,
        r.schema                                                    as schema,

        r.start                                                     as start_time,
        cast(r.start as date)                                       as start_date,

        r.status                                                    as status,
        r.table                                                     as table,
        r.update_id                                                 as update_id,

        r.update_started                                            as update_started_time,
        cast(r.update_started as date)                              as update_started_date,


        -- meta
        r.meta_delivery_time                                        as meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time,
        'freshdesk'                                                 as meta_source,
        1                                                           as meta_is_valid
from {{ ref('fivetran_audit_raw') }} r
