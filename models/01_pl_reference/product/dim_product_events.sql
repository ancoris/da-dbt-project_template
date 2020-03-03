{{
  config(
      materialized='scd2_events',
      schema='pl_reference',
      check_cols=[
          'name', 'product_type'
      ],

      natural_key_col='product_natural_key',
      modified_time='meta_process_time',
      created_time='product_launch_time',
      partition_by='date(meta_process_time)'
  )
}}
select  product_natural_key,

        -- attributes
        name,
        product_type,
        product_launch_time,

        -- meta fields
        meta_process_time,
        meta_delivery_time
from    {{ ref('dim_product_stage') }}
where   meta_process_time =  {{ dbt_macros.meta_process_time() }}
