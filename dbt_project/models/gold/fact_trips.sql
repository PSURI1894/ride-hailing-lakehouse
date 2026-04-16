{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

WITH silver_trips AS (
    SELECT * FROM {{ source('silver', 'trips') }}
)

SELECT
    trip_id,
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    trip_duration_minutes,
    fare_per_mile,
    CURRENT_TIMESTAMP() AS updated_at
FROM silver_trips

{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
