{{
    config(
        materialized='table'
    )
}}

select
    service_type,
    extract(YEAR from pickup_datetime) as year,
    extract(MONTH from pickup_datetime) as month,
    extract(QUARTER from pickup_datetime) as quarter,
    case 
        when extract(MONTH from pickup_datetime) between 1 and 3 then concat(extract(YEAR from pickup_datetime), '/', 'Q1')
        when extract(MONTH from pickup_datetime) between 4 and 6 then concat(extract(YEAR from pickup_datetime), '/', 'Q2')
        when extract(MONTH from pickup_datetime) between 7 and 9 then concat(extract(YEAR from pickup_datetime), '/', 'Q3')
        when extract(MONTH from pickup_datetime) between 10 and 12 then concat(extract(YEAR from pickup_datetime), '/', 'Q4')
    end as year_quarter,
    COALESCE(SUM(fare_amount + extra + mta_tax + tip_amount + tolls_amount + ehail_fee + improvement_surcharge + total_amount), 0) as revenue_quarterly_total_amount
    from 
        {{ ref('fact_trips') }}
    group by all