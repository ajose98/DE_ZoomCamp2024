{{
    config(
        materialized='table'
    )
}}

select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone -- where we find the value "Boro", it should actually be "Green" - let's change it
from {{ ref('taxi_zone_lookup') }}