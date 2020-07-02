{{
    config(
        materialized='incremental',
        unique_key='rental_id',
        schema='pl_journeys',
        partition_by = {'field': 'date(meta_process_time)',
          'data_type':'date'},
        cluster_by='rental_id',
        tags=["pl_journeys", "fact"]
    )
}}
select  j.rental_id,

        -- time and date
        j.start_date                                    as start_time,
        start_date_dim.date_surrogate_key               as start_date_surrogate_key,
        j.end_date                                      as end_time,
        end_date_dim.date_surrogate_key                 as end_date_surrogate_key,

        -- cycle station
        start_station_dim.cycle_station_surrogate_key   as start_station_surrogate_key,
        j.start_station_id                              as start_station_natural_key,
        start_station_dim.name                          as start_station_name,
        end_station_dim.cycle_station_surrogate_key     as end_station_surrogate_key,
        j.end_station_id                                as end_station_natural_key,
        end_station_dim.name                            as end_station_name,

        -- cycle
        bike_id                                         as cycle_id,

        -- measures
        1                                               as journey_count,
        duration                                        as duration_secs,
        duration/60                                     as duration_minutes,
        case
          when  start_station_dim.cycle_station_natural_key = end_station_dim.cycle_station_natural_key then 1 else 0
        end                                             as is_round_trip,
        st_geogpoint( start_station_dim.longitude,
                      start_station_dim.latitude)       as start_station_geo,
        st_geogpoint( end_station_dim.longitude,
                      end_station_dim.latitude)         as end_station_geo,
        st_distance(
          st_geogpoint( start_station_dim.longitude,
                        start_station_dim.latitude),
          st_geogpoint(-0.1, 51.5))/1000                as start_station_distance_from_city_center,
        st_distance(
          st_geogpoint( end_station_dim.longitude,
                        end_station_dim.latitude),
          st_geogpoint(-0.1, 51.5))/1000                as end_station_distance_from_city_center,

        st_distance(
          st_geogpoint( start_station_dim.longitude,
                        start_station_dim.latitude),
          st_geogpoint( end_station_dim.longitude,
                        end_station_dim.latitude))/1000 as distance_between_stations,

        -- meta
        j.meta_process_time,
        j.meta_delivery_time,
        'cycle_hire'                                    as meta_source
from {{ ref('cycle_hire_clean') }} j

left outer join {{ ref('dim_cycle_station') }} start_station_dim
  on start_station_dim.cycle_station_natural_key = j.start_station_id
  and j.start_date >= start_station_dim.meta_start_time and j.start_date < start_station_dim.meta_end_time

left outer join {{ ref('dim_cycle_station') }} end_station_dim
  on end_station_dim.cycle_station_natural_key = j.end_station_id
  and j.start_date >= end_station_dim.meta_start_time and j.start_date < end_station_dim.meta_end_time

left outer join {{ ref('dim_date') }} start_date_dim
  on start_date_dim.date_actual = cast(j.start_date as date)

left outer join {{ ref('dim_date') }} end_date_dim
  on end_date_dim.date_actual = cast(j.end_date as date)

where j.meta_process_time =  {{ meta_process_time() }}

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
and j.meta_delivery_time > (select ifnull( max(meta_delivery_time), {{CONSTANT_TIMESTAMP_SMALL()}}) from {{ this }})

{% endif %}
