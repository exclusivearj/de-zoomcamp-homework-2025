/*
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

    Up to 1 mile
    In between 1 (exclusive) and 3 miles (inclusive),
    In between 3 (exclusive) and 7 miles (inclusive),
    In between 7 (exclusive) and 10 miles (inclusive),
    Over 10 miles
*/

select 
count (*) FILTER(WHERE trip_distance <= 1) less_than_1,
count (*) FILTER(WHERE trip_distance > 1 AND trip_distance <= 3) gt_1_le_3,
count (*) FILTER(WHERE trip_distance > 3 AND trip_distance <= 7) gt_3_le_7,
count (*) FILTER(WHERE trip_distance > 7 AND trip_distance <= 10) gt_7_le_10,
count (*) FILTER(WHERE trip_distance > 10) gt_10
from green_taxi_data_polars
where DATE(lpep_pickup_datetime) >= '2019-10-01' AND DATE(lpep_dropoff_datetime) < '2019-11-01';

/*
"less_than_1"	"gt_1_le_3"	"gt_3_le_7"	"gt_7_le_10"	"gt_10"
104802	198924	109603	27678	35189
*/