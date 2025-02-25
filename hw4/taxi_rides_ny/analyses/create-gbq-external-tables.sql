CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.green_tripdata`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-hw3-akshay/green/*.parquet']
    );

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.yellow_tripdata`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-hw3-akshay/yellow/*.parquet']
    );

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-hw3-akshay/fhv/*.parquet']
    );