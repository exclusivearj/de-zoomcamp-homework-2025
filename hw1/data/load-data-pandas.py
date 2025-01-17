#!/usr/bin/env python

# All imports go here
import pandas as pd
from sqlalchemy import create_engine, text,  CursorResult
import psycopg
from time import time


# endpoint = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"
endpoint = "hw1/data/yellow_tripdata_2019-01.csv.gz"

user="postgres"
password="postgres"
db_name = "ny_taxi"
hostname="localhost"
DATABASE_URL = f"postgresql+psycopg://{user}:{password}@{hostname}:5433/{db_name}"

# Create an engine instance
engine = create_engine(DATABASE_URL)

# Create the table
# Get the schema
schema = print(pd.io.sql.get_schema(df, name = "yellow_taxi_data", con = engine))
print(schema)

# Write the results to db
df_iter = pd.read_csv(endpoint, iterator=True, chunksize=100000)
# df_iter = pd.read_csv("hw1/data/small.csv", iterator=True, chunksize=100000)

while (df := next(df_iter, None)) is not None:
    t_start = time()
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    t_end = time()
    print('inserted another chunk, took %.3f second' % (t_end - t_start))

'''
# Get all the data
with engine.connect() as connection:
    result:list = connection.execute(text("SELECT * FROM yellow_taxi_data;")).all()
    total_rows = len(result)
    print(f"Total rows: [{total_rows}]")
    for index, row in enumerate(result, start=1):
        print(f"({index} of {total_rows}), [{row}]")
'''

# Get row count
with engine.connect() as connection:
    result:list = connection.execute(text("SELECT count(1) FROM yellow_taxi_data;")).all()
    total_rows = len(result)
    print(f"Total rows: [{total_rows}]")
    for index, row in enumerate(result, start=1):
        print(f"({index} of {total_rows}), [{row}]")

