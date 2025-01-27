/*
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.
*/
with longest_trip_dist AS (
	select 
	max(trip_distance) max_trip_dist
	from green_taxi_data_polars
)
select date(lpep_pickup_datetime) pickup_date
from green_taxi_data_polars
where trip_distance = (select max_trip_dist from longest_trip_dist)

/*
"pickup_date"
"2019-10-31"
*/