{{config(materialized='table')}}

WITH data as (
SELECT
    pickup_locationid,
    dropoff_locationid,
    dropoff_zone,
    pickup_year,
    pickup_month,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
    PERCENTILE_CONT(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.90) OVER (PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p90
from 
    {{ ref('dim_fhv_trips') }}
where 
    pickup_year = 2019 
    and pickup_month = 11
    and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East') 
)
select 
    dropoff_zone,
    trip_duration,
    p90
from data
order by
    p90 desc
limit 10