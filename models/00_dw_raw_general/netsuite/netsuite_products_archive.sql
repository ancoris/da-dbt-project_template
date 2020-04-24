{{
    config(
        materialized='archive_incremental',
        schema='raw_archive',
        partition_by='date(meta_process_time)'
    )
}}
select  product_id,
        name,
        product_type,
        product_launch_time,
        product_category,

        -- meta fields
        {{dbt_macros.meta_process_time() }} as meta_process_time,
        snapshot_time as meta_delivery_time
from {{ ref('netsuite_products_raw') }}
