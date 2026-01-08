import logging
import sys
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_SCHEMA = 'raw'

def load_csv_to_db(
    file_path: Path, 
    table_name: str, 
    columns: list[str], 
    conn_id: str,
    truncate: bool = True
) -> None:
    """
    Ingests CSV into Postgres 'raw' schema. 
    Optimized via COPY for ELT workflows.
    """
    # 1. Instantiate Hook first
    hook = PostgresHook(postgres_conn_id=conn_id)
    
    # 2. Validate path
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    
    full_table_name = f"{DEFAULT_SCHEMA}.{table_name}"

    # 3. Handle Idempotency
    if truncate:
        logging.info(f"Truncating {full_table_name}")
        hook.run(f"TRUNCATE TABLE {full_table_name};")

    # Format the columns for the SQL query: (col1, col2, col3)
    # We EXCLUDE 'ingested_at' here so the DB uses the DEFAULT value.
    cols_str = f"({', '.join(columns)})"
    
    # 4. Define Query
    query = f"""
        COPY {full_table_name} {cols_str}
        FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',');
    """

    try:
        logging.info(f"Loading {file_path} into {full_table_name}")
        hook.copy_expert(sql=query, filename=str(path))
    except Exception as e:
        logging.error(f"Error loading data into {full_table_name}: {e}")
        # Re-raise so Airflow marks the task as failed
        raise

# This allows you to run: python scripts/load.py
# for local debugging outside of Airflow.
if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    # Example test call
    try:
        load_csv_to_db(
            file_path=Path("../data/olist_customers)dataset.csv"),
            table_name="olist_customers",
            conn_id="olist" 
        )
    except Exception:
        sys.exit(1)

    print("Data load completed successfully.")