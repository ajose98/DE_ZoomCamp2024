{{
    config(
        materialized='view'
    )
}}
 
select 
    dispatching_base_num,
    sr_flag,
    affiliated_base_number,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    -- identifiers
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
from {{ source('staging','fhv_tripdata') }}
where extract(year from pickup_datetime) = 2019

-- this is a useful piece of code that can be converted to a macro (this is called a dev limit)
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}