{{config(materialized='table')}}

/*
1. Create a new model `fct_taxi_trips_monthly_fare_p95.sql`
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`)
3. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month

Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?
*/

WITH cte AS (
  SELECT
    service_type,
    fare_amount
  FROM
    {{ ref('fact_trips') }}
  where 
    pickup_datetime BETWEEN '2020-04-01' AND '2020-04-30'
    AND fare_amount > 0
    AND trip_distance > 0
    AND lower(payment_type_description) in ('cash', 'credit card')
)

select
  DISTINCT
  service_type,
  PERCENTILE_CONT(fare_amount, 0.97) OVER(PARTITION BY service_type) as p97,
  PERCENTILE_CONT(fare_amount, 0.95) OVER(PARTITION BY service_type) as p95,
  PERCENTILE_CONT(fare_amount, 0.90) OVER(PARTITION BY service_type) as p90
from 
  cte