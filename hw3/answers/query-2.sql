/*
Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

    18.82 MB for the External Table and 47.60 MB for the Materialized Table
    0 MB for the External Table and 155.12 MB for the Materialized Table
    2.14 GB for the External Table and 0MB for the Materialized Table
    0 MB for the External Table and 0MB for the Materialized Table
*/

-- External table
SELECT
  COUNT (DISTINCT PULocationID) distinct_pu_location_id_count
FROM
  `second-academy-449820-u6.de_zoomcamp_hw3.yellow_trip_taxi_records_external`
-- 0 MB


-- Regular table
SELECT
  COUNT (DISTINCT PULocationID) distinct_pu_location_id_count
FROM
  `second-academy-449820-u6.de_zoomcamp_hw3.yellow_trip_taxi_records_regular_unpartitioned`
-- 155.12 MB