{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [
        'company_id',
        'domain',
        '_fivetran_deleted',

        '_fivetran_synced_time',
        '_fivetran_synced_date',

        ],
        natural_key_col = 'company_domain_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = 'company_domain_natural_key',
        partition_by    = {'field': 'date(meta_process_time)',
                            'data_type':'date'}
    )
}}

select c.company_id          as company_domain_natural_key,  /* CHECK THIS */
        c.company_id,
        c.domain,
        c._fivetran_deleted,

        c._fivetran_synced_time,
        c._fivetran_synced_date,

        cast('2000-1-1' as date)                                    as create_date,

        -- meta
        c.meta_source,
        c.meta_delivery_time,
        {{meta_process_time()}}                                     as meta_process_time
from {{ ref('company_domain_clean') }} c
where c.meta_is_valid = 1