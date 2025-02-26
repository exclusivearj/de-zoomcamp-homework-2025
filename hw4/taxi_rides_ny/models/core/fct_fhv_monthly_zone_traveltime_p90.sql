{{config(materialized='table')}}

with base AS (
    select
        *,
        TIME_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from
        {{ ref('dim_fhv_trips') }}
),
percentile AS (
    select
        pickup_year,
        pickup_month,
        pickup_zone,
        dropoff_zone,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] as traveltime_p90
    from
        base
    group by
        1, 2, 3, 4
)