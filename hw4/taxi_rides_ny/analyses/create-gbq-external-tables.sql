-- Run these SQL in the GBQ studio to create external tables for the taxi rides data
CREATE EXTERNAL TABLE `trips_data_all.green_tripdata`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-hw3-akshay/green/green_tripdata_*.parquet']
    );

CREATE EXTERNAL TABLE `trips_data_all.yellow_tripdata`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-hw3-akshay/yellow/yellow_tripdata_*.parquet']
    );

CREATE EXTERNAL TABLE `trips_data_all.fhv_tripdata`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-hw3-akshay/fhv/fhv_tripdata_*.parquet']
    );