# DE Zoomcamp HW4

Description here --> https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/04-analytics-engineering/homework.md

# Setup
- Create python virtual environment
```python
python3 -m venv .venv
```

- Install `dbt` with `postgres` plugin:
```python
python -m pip install dbt-core dbt-postgres
```

- Setup dbt profile
Create [profiles.yml](./homework/profiles.yml) with connection details

- Run `dbt init` with profile name = `dbt-postgres`
```bash
dbt init dbt-postgres
```

- Start the containers: [1-up.sh](/1-up.sh)

- Test the connection: `dbt debug`
    - `dbt` should be able to connect to both `git` and local `postgres`

# Answers
All answers are located in the [answers folder](./answers/)

# Contact
Contact [Akshay Jain](https://www.linkedin.com/in/akshayrjain/)