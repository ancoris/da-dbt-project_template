  -- depends_on: {{ ref('ml_station_distance_from_city_center_kmeans_model') }}
  {{
    config(
      materialized='view',
      schema='pl_journeys'
    )
}}
select  centroid_id,
        station_name,
        station_distance_from_city_center
from  {{this.database}}.{{this.schema}}.ml_station_distance_from_city_center_kmeans_output
