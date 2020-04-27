{{
    config(
      materialized='table',
      schema='pl_journeys'
    )
}}
select  name                                      as station_name,
        st_distance(
          st_geogpoint( longitude, latitude),
          st_geogpoint(-0.1, 51.5)
        )/1000                                    as station_distance_from_city_center
from  {{ref('dim_cycle_station')}}
