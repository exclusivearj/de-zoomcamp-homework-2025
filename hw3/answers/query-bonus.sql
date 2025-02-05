/**
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
*/

SELECT
  COUNT(1)
FROM
  `second-academy-449820-u6.de_zoomcamp_hw3.yellow_trip_taxi_records_regular_unpartitioned`;

0 bytes, data was already processed and stored in the materialized table.