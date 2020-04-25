{{
    config(
      materialized='ml_model_kmeans',
      schema='pl_journeys',
      feature_set_key='station_name',
      model_options='model_type="kmeans", num_clusters=6',
      tags=["ml_model"]
    )
}}
select  station_name,
        station_distance_from_city_center
from  {{ref('ml_station_distance_from_city_center_kmeans_features')}} s
