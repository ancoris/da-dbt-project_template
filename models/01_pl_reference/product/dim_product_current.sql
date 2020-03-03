{{
    config(
        materialized='view',
        schema='pl_reference'
    )
}}

select  product_surrogate_key,
        product_natural_key,

        -- attributes
        name,
        product_type,
        product_launch_time,

        -- meta fields
        meta_process_time,
        meta_delivery_time,
        meta_scd_action,
        meta_start_time,
        meta_end_time
from    {{ ref('dim_product') }}
where meta_is_latest = 1
