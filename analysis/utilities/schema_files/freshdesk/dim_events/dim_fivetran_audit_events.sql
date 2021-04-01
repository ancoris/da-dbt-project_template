{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'id',

        '_fivetran_synced_time',
        '_fivetran_synced_date',


        'done_time',
        'done_date',

        'message',

        'progress_time',
        'progress_date',

        'rows_updated_or_inserted',
        'schema',

        'start_time',
        'start_date',

        'status',
        'table',
        'update_id',

        'update_started_time',
        'update_started_date',

        ],
        natural_key_col = 'fivetran_audit_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'fivetran_audit_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.id          as fivetran_audit_natural_key,  /* CHECK THIS */
        c.id,

        c._fivetran_synced_time,
        c._fivetran_synced_date,


        c.done_time,
        c.done_date,

        c.message,

        c.progress_time,
        c.progress_date,

        c.rows_updated_or_inserted,
        c.schema,

        c.start_time,
        c.start_date,

        c.status,
        c.table,
        c.update_id,

        c.update_started_time,
        c.update_started_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('fivetran_audit_clean') }} c
where c.meta_is_valid = 1