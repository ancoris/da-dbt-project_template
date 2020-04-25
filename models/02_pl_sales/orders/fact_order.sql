{#
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        schema='pl_sales',
        cluster_by='order_id',
        partition_by='date(meta_process_time)'
    )
}}
select  o.order_id,

        -- time and date
        o.order_time,
        order_date_dim.date_surrogate_key,

        -- product dim
        prod_dim.product_surrogate_key,
        o.product_id  as order_product_id,
        prod_dim.name,
        prod_dim.product_type,

        -- measures
        1 as order_count,
        1 as product_qty,

        -- meta
        o.meta_process_time,
        o.meta_delivery_time,
        'eshop.shopify' as meta_source

from {{ ref('shopify_orders_clean') }} o

left outer join {{ ref('dim_product') }} prod_dim
  on prod_dim.product_natural_key = o.product_id
  and o.order_time >= prod_dim.meta_start_time and o.order_time <prod_dim.meta_end_time

left outer join {{ ref('dim_date') }} order_date_dim
  on order_date_dim.date_actual = cast(o.order_time as date)

where o.meta_process_time =  {{ dbt_macros.meta_process_time() }}

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
and o.meta_delivery_time > (select ifnull( max(meta_delivery_time), {{dbt_macros.CONSTANT_TIMESTAMP_SMALL()}}) from {{ this }})

{% endif %}
#}
