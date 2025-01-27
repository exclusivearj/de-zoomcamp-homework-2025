/*
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
Consider only lpep_pickup_datetime when filtering by date.
*/
with pickup_location_total_amount AS (
select 
"PULocationID" pickup_location_id,
sum(total_amount) total_amount
from 
green_taxi_data_polars
where date(lpep_pickup_datetime) = '2019-10-18'
group by 1
)

select
pickup_location_id, z."Zone"
from 
pickup_location_total_amount p
inner join taxi_zone_data_polars z
on (p.pickup_location_id = z."LocationID")
where total_amount > 13000

/*
"pickup_location_id"	"Zone"
74	"East Harlem North"
75	"East Harlem South"
166	"Morningside Heights"
*/