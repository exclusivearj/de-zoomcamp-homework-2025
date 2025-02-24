# DE Zoomcamp HW4

Description here --> https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/04-analytics-engineering/homework.md

# Setup
- Create python virtual environment
```python
python3 -m venv .venv
```

## IMP for data setup
- Use [this script](./taxi_rides_ny/analyses/web_to_gcs.py) to load data from Github to GCS
- Create external GBQ table
- Upload the [zone lookup file](./taxi_rides_ny/taxi_zone_lookup.csv) to GCS and create a native table for it

## `dbt` setup
- Install `dbt` with `postgres` plugin:
```python
python -m pip install dbt-core dbt-bigquery
```

- Setup dbt profile
Create [profiles.yml](./homework/profiles.yml) with connection details

- Run `dbt init` with profile name = `taxi_rides_ny_profile`
```bash
dbt init taxi_rides_ny_profile
```

- Compile the code
```bash
dbt clean; dbt deps; dbt compile; dbt run; dbt test
```

# Answers
All answers are located in the [answers folder](./answers/)

# Contact
Contact [Akshay Jain](https://www.linkedin.com/in/akshayrjain/)