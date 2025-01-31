# de-zoomcamp-homework-2025
Akshay Jain's homework for DE zoomcamp for 2025

## Homework 2
See questions at https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/02-workflow-orchestration/homework.md

## Answers
The answers are in the [./hw2/answers folder](./hw2/answers)

### Notes
#### Download data from here:
- Yellow --> https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/
- Green --> https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/

#### Bulk download flows
```bash
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/01_getting_started_data_pipeline.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/02_postgres_taxi.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/02_postgres_taxi_scheduled.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/03_postgres_dbt.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/04_gcp_kv.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/05_gcp_setup.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/06_gcp_taxi.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/06_gcp_taxi_scheduled.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/07_gcp_dbt.yaml
```

#### How to get IP address of the postgres container to connect in pgadmin
```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' zoomcamp-postgres-1
```

# Contact
Feel free to reach out to Akshay Jain on LinkedIn at https://www.linkedin.com/in/akshayrjain/