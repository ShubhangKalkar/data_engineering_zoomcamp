with source as (

    select * from {{ source('raw', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        sr_flag

    from source
    where dispatching_base_num is not null   -- homework requirement

)

select * from renamed
