# DE Zoomcamp HW4

Description here --> https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/04-analytics-engineering/homework.md

# Setup
- Create python virtual environment
```python
python3 -m venv .venv
```

## IMP for data setup
- Use [this script](./taxi_rides_ny/analyses/web_to_gcs.py) to load data from Github to GCS
- Create external GBQ table using [this sql file](./taxi_rides_ny/analyses/create-gbq-external-tables.sql)
- Upload the [zone lookup file](./taxi_rides_ny/taxi_zone_lookup.csv) to GCS and create a native table for it

## `dbt` setup
- Install all dependencies
```python
pip install -r requirements.txt
```

- Setup dbt profile
Create [profiles.yml](./homework/profiles.yml) with connection details

- Run `dbt init` with profile name = `taxi_rides_ny_profile`
```bash
dbt init taxi_rides_ny_profile
```

- Create the staging views
```bash
dbt clean; dbt deps; dbt compile; dbt run
```
    - This should create some views in the dataset

- Run dbt test
```bash
dbt test
```
All tests should run successfully

- To build a specific dbt model:
```bash
dbt build --select <model-name>
```
    For ex:
    ```bash
    dbt build --select fact_trips
    ```


# Answers
All answers are located in the [answers folder](./answers/)

# Contact
Contact [Akshay Jain](https://www.linkedin.com/in/akshayrjain/)