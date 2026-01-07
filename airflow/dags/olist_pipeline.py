from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from pathlib import Path

from src.extract import download_raw_data

# Define paths as constants for Robustness (DRY principle)
AIRFLOW_CONTAINER_ROOT = Path("/opt/airflow")
SQL_FOLDER = AIRFLOW_CONTAINER_ROOT / "scripts" / "sql"

with DAG(
    dag_id='olist_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    description='This DAG handles the EL in the Olist data pipeline.',
    catchup=False,

    # This tells Airflow where to look for your SQL files
    template_searchpath=[str(SQL_FOLDER)] 

) as dag:

    @task 
    def extract_olist_data(kaggle_dataset_id: str, target_path: str):
        download_raw_data(kaggle_dataset_id, target_path)

    extract_data = extract_olist_data("olistbr/brazilian-ecommerce", f"{AIRFLOW_CONTAINER_ROOT}/data")

    create_raw_tables = SQLExecuteQueryOperator(
        task_id='create_raw_tables',
        conn_id='olist_connection',
        database='olist',
        sql='create_raw_tables.sql' 
    )

    # # Adding the Load step as well
    # load_data = BashOperator(
    #     task_id='load_data',
    #     bash_command=f"{VENV_PYTHON} {PROJECT_ROOT}/src/load.py"
    # )

    extract_data >> create_raw_tables