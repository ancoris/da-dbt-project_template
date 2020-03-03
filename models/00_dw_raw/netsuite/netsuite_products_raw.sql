{{
    config(
        materialized='view',
        schema='raw_netsuite'
    )
}}
select  snapshot_time,
        product_id,
        name,
        product_type,
        product_launch_time,
        'test' as product_category
from {{ source('london_bicycles', 'cycle_hire') }}
