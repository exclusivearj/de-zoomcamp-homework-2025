#!/usr/bin/env python
# coding: utf-8


# All imports go here
import polars as pl
from time import time


import adbc_driver_postgresql.dbapi
from adbc_driver_manager.dbapi import Connection


# Constants
user="postgres"
password="postgres"
db_name = "ny_taxi"
port=5433
hostname="localhost"
DATABASE_URL = f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
# TABLE_NAME="yellow_taxi_data_polars"
TABLE_NAME="green_taxi_data_polars"
ZONE_TABLE_NAME="taxi_zone_data_polars"
# ENDPOINT = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"
# ENDPOINT = "hw1/data/yellow_tripdata_2019-01.csv.gz"
# ENDPOINT = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
ENDPOINT = "hw1/data/green_tripdata_2019-10.csv.gz"
ZONEDATA = "hw1/data/taxi_zone_lookup.csv"


# Create a cursor instance
conn: Connection = adbc_driver_postgresql.dbapi.connect(DATABASE_URL)

# Schema for green taxi data
def green_taxi_polars_schema() -> dict:
  return {
    "VendorID": pl.Int32,
    "lpep_pickup_datetime": pl.Datetime,
    "lpep_dropoff_datetime": pl.Datetime,
    "store_and_fwd_flag": pl.String,
    "RatecodeID": pl.Int8,
    "PULocationID": pl.Int32,
    "DOLocationID": pl.Int32,
    "passenger_count": pl.Int8,
    "trip_distance": pl.Float64,
    "fare_amount": pl.Float64,
    "extra": pl.Float64,
    "mta_tax": pl.Float64,
    "tip_amount": pl.Float64,
    "tolls_amount": pl.Float64,
    "ehail_fee": pl.Float64,
    "improvement_surcharge": pl.Float64,
    "total_amount": pl.Float64,
    "payment_type": pl.Int8,
    "trip_type" : pl.Int8,
    "congestion_surcharge": pl.Float64,
  }

# Schema for Taxi zone data
def taxi_zone_polars_schema() -> dict:
  return {
    "LocationID": pl.Int32,
    "Borough": pl.String,
    "Zone": pl.String,
    "service_zone": pl.String,
  }

# Debug
# df = pl.read_csv(ENDPOINT, schema_overrides=green_taxi_polars_schema(), n_rows=1000)
# df

# Helper function to recreate a table in PostgreSQL
def recreate_table(conn: Connection, schema: dict, table_name: str) -> None:
    """
    Creates a PostgreSQL table for yellow taxi data using ADBC engine.
    
    Args:
        connection_string: PostgreSQL connection string
        table_name: Name of the table to create
    """
    # Map Polars types to PostgreSQL types
    pg_type_mapping = {
        pl.Int32: "INTEGER",
        pl.Int8: "SMALLINT",
        pl.Float64: "DOUBLE PRECISION",
        pl.Datetime: "TIMESTAMP",
        pl.String: "VARCHAR"
    }
    
    # Convert schema to PostgreSQL column definitions
    columns = []
    for column_name, polars_type in schema.items():
        quoted_column_name = f'"{column_name}"'
        pg_type = pg_type_mapping[polars_type]
        columns.append(f"{quoted_column_name} {pg_type}")
    
    # Drop the table if it already exists
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    print(f"Drop table sql ==> {drop_table_sql}")
    # Create a cursor instance
    with conn.cursor() as cur:
        cur.execute(drop_table_sql)
        conn.commit()
    
    # Create the table definition SQL
    nl = ",\n        "
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {nl.join(columns)}
        );
        """
    
    print(f"Create table sql ==> {create_table_sql}")
    
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        conn.commit()

# Helper function to read data in batches and insert into the database
def batch_read_and_insert_into_db(source: str, schema: dict, table_name: str) -> None:
  batch_reader = pl.read_csv_batched(
      source = source,
      batch_size=100000,
      schema_overrides=schema,
      has_header=True,
      separator=",",
      try_parse_dates=True
  )
  batches = batch_reader.next_batches(n=10000)
  start_time = time()
  for df in batches:
    t_start = time()
    df.write_database(
      table_name,
      connection=conn,
      engine="adbc",
      if_table_exists="append",
    )
    t_end = time()
    print('inserted another chunk, took %.3f second' % (t_end - t_start))
  end_time = time()
  print(f"Final insertion took {end_time - start_time} seconds")

# Helper function to get row count of a table
def get_row_count(table_name: str)->int:
  with conn.cursor() as cur:
    cur.execute(f"SELECT count(1) FROM {table_name};")
    # print(cur.fetch_arrow_table())
    return cur.fetchone()[0]

# recreate_table(conn, green_taxi_polars_schema(), TABLE_NAME)
# recreate_table(conn, taxi_zone_polars_schema(), ZONE_TABLE_NAME)

# Insert green taxi data
batch_read_and_insert_into_db(ENDPOINT, green_taxi_polars_schema(), TABLE_NAME)

# Insert taxi zone data
batch_read_and_insert_into_db(ZONEDATA, taxi_zone_polars_schema(), ZONE_TABLE_NAME)

# Get row count for green taxi data
get_row_count(TABLE_NAME)

# Get row count for taxi zone data
get_row_count(ZONE_TABLE_NAME)
