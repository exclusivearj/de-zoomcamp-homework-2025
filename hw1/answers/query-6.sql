/*
For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?
Note: it's tip , not trip
We need the name of the zone, not the ID.
*/
with drop_location_highest_tip AS (
select
g."DOLocationID" drop_location_id,
tip_amount
from green_taxi_data_polars g
join taxi_zone_data_polars z
on ((g."PULocationID" = z."LocationID") AND (z."Zone" = 'East Harlem North'))
where date(lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
order by 2 desc
limit 1
)
select 
z."Zone" zone_name
from 
drop_location_highest_tip d
join taxi_zone_data_polars z
on (d.drop_location_id = z."LocationID")

/*
"zone_name"
"JFK Airport"
*/