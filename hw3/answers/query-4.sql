/*
Question 4:

How many records have a fare_amount of 0?

    128,210
    546,578
    20,188,016
    8,333
*/

SELECT
  COUNT(1) c
FROM
  `second-academy-449820-u6.de_zoomcamp_hw3.yellow_trip_taxi_records_regular_unpartitioned`
WHERE
  fare_amount = 0

-- 8,333