{{ config(materialized='table') }}

with pickup_zones as (
    select distinct
        PULocationID as location_id,
        pickup_zone as zone_name,
        pickup_borough as borough,
        pickup_h3 as zone_h3
    from {{ source('silver', 'trips') }}
),
dropoff_zones as (
    select distinct
        DOLocationID as location_id,
        dropoff_zone as zone_name,
        dropoff_borough as borough,
        dropoff_h3 as zone_h3
    from {{ source('silver', 'trips') }}
),
combined as (
    select * from pickup_zones
    union
    select * from dropoff_zones
)

select distinct *
from combined
