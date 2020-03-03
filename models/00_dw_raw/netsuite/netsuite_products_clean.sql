{{
    config(
        materialized='view',
        schema='raw_clean'
    )
}}
select  product_id,
        name,
        product_type,
        product_launch_time,

        -- meta fields
        meta_process_time,
        meta_delivery_time
from {{ ref('netsuite_products_archive') }}
