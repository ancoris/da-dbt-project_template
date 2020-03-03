{{
    config(
        materialized='table',
        schema='pl_reference_stage'
    )
}}
select  product_id as product_natural_key,

        -- attributes
        name,
        product_type,
        product_launch_time,

        -- meta fields
        meta_process_time,
        meta_delivery_time
from    {{ ref('netsuite_products_clean') }}
where   meta_process_time =  {{ dbt_macros.meta_process_time() }}
