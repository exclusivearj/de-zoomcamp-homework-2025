# de-zoomcamp-homework-2025
Akshay Jain's homework for DE zoomcamp for 2025

## Homework 1
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

# Contact
Feel free to reach out to Akshay Jain on LinkedIn at https://www.linkedin.com/in/akshayrjain/