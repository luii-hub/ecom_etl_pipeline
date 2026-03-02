# 🌀 olist: Brazilian E-Commerce Data Pipeline
![Olist Dataset](docs/dataset-cover.png)

This project implements a containerized ETL pipeline for the Olist Brazilian E-Commerce dataset. As a Data Engineer, the goal was to take raw, highly relational CSV data and transform it into an analytics-ready Star Schema within a PostgreSQL Data Warehouse. The core design follows the Medallion Architecture, providing a logical framework for data quality and refinement as it moves from raw ingestion to business-ready insights.

For a full developer documentation, please refer to [here](/docs/docs.md).

### Project Overview
<img width="100%" height="100%" alt="image" src="https://github.com/user-attachments/assets/a422dd04-bc07-4bfa-8a7e-c2820409ac97" />

### 🛠️ Tech Stack
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)


## 📁 Project Structure

```
project-root/
├── airflow/
│ ├── dags/
├── data/
│ └── *.csv
├── dbt/
│ └── olist_ecommerce/
│ └── logs/
│ └── *.yaml
├── docs/
├── scripts/
│ └── sql/
├── src/
│ └── *.py
├── .env
└── .gitignore
└── requirements.txt
```

## 📦 How to use the Repository (For Windows)

1. Install WSL, Dbeaver, Docker Desktop on local machine
2. Clone the repository
3. Build and start all services defined in docker-compose.yml

    ```
    docker compose up -d
    ```

    Check if the containers are created

    ```
    docker ps
    ```
    
    If there are changes in the yml file recreate the containers

    ```
    docker compose up -d --build
    ```

4. Initialize Airflow API Server
        
    ```
    docker compose run <api-server-container-name> airflow db init
    ```
    Rerun the container

    ```
    docker compose up -d
    ```

    Access the Airflow through http://localhost:8080 then log in using
    ```
    Username: <username>
    Password: <password>
    ```

5. Connect and test PostgreSQL database to dbeaver
    https://dbeaver.com/docs/dbeaver/Create-Connection/#use-the-new-connection-wizard

6. VitalDBT VS Code Plug-ins <br>
    Power User for dbt <br>
