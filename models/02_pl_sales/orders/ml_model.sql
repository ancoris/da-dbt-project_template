{#
{{
    config(
      materialized='ml_model_kmeans',
      schema='pl_journeys',
      feature_set_key='rental_id',
      model_options='model_type="kmeans", num_clusters=6, kmeans_init_method = "KMEANS++"',
      tags=["ml_model"]
    )
}}
select  rental_id,
        duration_minutes,
        is_round_trip,
        start_station_distance_from_city_center,
        distance_between_stations
from    {{ref('fact_order')}} f
where f.start_station_surrogate_key is not null
and f.end_station_surrogate_key is not null
#}
