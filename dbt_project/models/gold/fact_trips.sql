{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

with silver_trips as (
    select *
    from {{ source('silver', 'trips') }}
),
final as (
    select
        trip_id,
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID as rate_code_id,
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        trip_duration_minutes,
        fare_per_mile,
        pickup_zone,
        dropoff_zone,
        pickup_borough,
        dropoff_borough,
        pickup_h3,
        dropoff_h3,
        _ingest_ts
    from silver_trips
)

select * from final

{% if is_incremental() %}
where _ingest_ts > (select coalesce(max(_ingest_ts), cast('1900-01-01' as timestamp)) from {{ this }})
{% endif %}
