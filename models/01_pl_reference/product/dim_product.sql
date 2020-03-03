{{
    config(
        materialized='scd2_history',
        schema='pl_reference',
        natural_key_col='product_natural_key'
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
        meta_start_time
from  {{ ref('dim_product_events') }}
