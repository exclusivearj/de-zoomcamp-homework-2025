# de-zoomcamp-homework-2025
Akshay Jain's homework for DE zoomcamp for 2025

## Homework 1
See questions at: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/01-docker-terraform/homework.md

### Notes
How to get IP address of the postgres container to connect in pgadmin
```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres
```
#### Start up
```bash
docker compose build
docker compose up -d # Starts containers in detached mode
```
#### Tear down / shutdown
```bash
docker compose down
```
- When you start the jupyter_notebook, you'd need to view the container's logs to fetch the token for login.
- Convert notebook to python script:
```bash
jupyter nbconvert --to script notebook.ipynb
```

#### Answers
The answers are in the [./hw1/answers folder](./hw1/answers)

# Contact
Feel free to reach out to Akshay Jain on LinkedIn at https://www.linkedin.com/in/akshayrjain/