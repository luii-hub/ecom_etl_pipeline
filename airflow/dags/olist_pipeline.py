from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from pathlib import Path

from src.extract import download_raw_data
from src.load import load_csv_to_db

# Define paths as constants for Robustness (DRY principle)
AIRFLOW_CONTAINER_ROOT = Path("/opt/airflow")
SQL_FOLDER = AIRFLOW_CONTAINER_ROOT / "scripts" / "sql"

# Mapping: "csv_filename": ("db_table_name", [list_of_csv_columns])
TABLE_CONFIG = {
    "olist_customers_dataset": (
        "olist_customers", 
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
    ),
    "olist_geolocation_dataset": (
        "olist_geolocation", 
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]
    ),
    "olist_order_items_dataset": (
        "olist_order_items", 
        ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
    ),
    "olist_order_payments_dataset": (
        "olist_order_payments",
        ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
    ),
    "olist_order_reviews_dataset": (
        "olist_order_reviews",
        ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"]
    ),
    "olist_orders_dataset": (
        "olist_orders", 
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]
    ),
    "olist_products_dataset": (
        "olist_products", 
        ["product_id", "product_category_name", "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    ),
    "olist_sellers_dataset": (
        "olist_sellers", 
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]
    ),
    "product_category_name_translation": (
        "olist_category_name_translation",
        ["category_name", "category_name_english"]
    )

}

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

    with TaskGroup(group_id="load_raw_olist") as load_raw_data_group:
        for csv_file, (table_name, columns) in TABLE_CONFIG.items():
            PythonOperator(
                task_id=f"load_{table_name}",
                python_callable=load_csv_to_db,
                op_kwargs={
                    "file_path": f"{AIRFLOW_CONTAINER_ROOT}/data/{csv_file}.csv",
                    "table_name": table_name,
                    "conn_id": "olist_connection",
                    "columns": columns, # Pass the column list here
                    "truncate": True
                }
            )

    extract_data >> create_raw_tables >> load_raw_data_group