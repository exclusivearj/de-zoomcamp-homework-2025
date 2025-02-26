{{config(materialized='table')}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
stg_fhv_tripdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
)

select 
    'FHV' as service_type,
    EXTRACT(YEAR FROM ft.pickup_datetime) as pickup_year,
    EXTRACT(MONTH FROM ft.pickup_datetime) as pickup_month,
    ft.dispatching_base_num as dispatching_base_num,
    ft.pickup_datetime as pickup_datetime,
    ft.dropOff_datetime as dropoff_datetime,
    ft.PUlocationID as pickup_locationid,
    ft.DOlocationID as dropoff_locationid,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.zone as dropoff_zone,
    ft.SR_Flag as sr_flag,
    ft.Affiliated_base_number as affiliated_base_number
from stg_fhv_tripdata ft
inner join dim_zones as pickup_zone
on ft.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on ft.DOlocationID = dropoff_zone.locationid