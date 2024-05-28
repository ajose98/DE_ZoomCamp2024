{{
    config(
        materialized='table'
    )
}}
-- by materializing this as a table we are making our queries more efficient and more performant

with fhv_tripdata as (
    Select *, 'Fhv' as service_type
    From {{ ref('stg_fhv_tripdata') }}
),
 dim_zones as (
    Select *
    From {{ ref('dim_zones') }}
    Where borough != 'Unknown'
 )

Select 
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  

From fhv_tripdata inner join dim_zones as pickup_zone on fhv_tripdata.pickup_locationid = pickup_zone.locationid
                inner join dim_zones as dropoff_zone on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid