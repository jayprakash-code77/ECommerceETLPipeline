from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
from sqlalchemy import create_engine
import kagglehub
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Database connection details
DB_URI = "postgresql://airflow:airflow@postgres:5432/ecommerce_db"

# Mapping of table names to corresponding CSV file names
CSV_FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv"
}

def download_dataset():
    """Downloads the dataset from KaggleHub and logs details."""
    logging.info("Downloading dataset from KaggleHub...")
    dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

    # Verify if the dataset was downloaded successfully
    if os.path.exists(dataset_path):
        logging.info(f"Dataset downloaded to: {dataset_path}")
        logging.info("Files in dataset directory:")
        for root, _, files in os.walk(dataset_path):
            for name in files:
                logging.info(f"  - {os.path.join(root, name)}")
    else:
        logging.error(f"Download path not found: {dataset_path}")

def extract_and_load():
    """Extracts data from KaggleHub cache and loads it into PostgreSQL."""
    engine = create_engine(DB_URI)

    # Ensure database connection
    try:
        with engine.connect() as conn:
            logging.info("Successfully connected to PostgreSQL.")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return
    
    # Download dataset path
    dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

    for table_name, file_name in CSV_FILES.items():
        file_path = os.path.join(dataset_path, file_name)

        try:
            # Check if the file exists before loading
            if os.path.exists(file_path):
                logging.info(f"Loading {file_name} into {table_name} table...")
                
                # Read CSV into DataFrame
                df = pd.read_csv(file_path)

                # Log column names to verify correctness
                logging.info(f"Columns in {file_name}: {list(df.columns)}")

                # Ensure 'price' column exists for 'order_items' table
                if table_name == "order_items" and "price" not in df.columns:
                    logging.error(f"'price' column missing in {file_name}. Aborting load.")
                    return
                
                # Convert 'price' column to float if present
                if "price" in df.columns:
                    df["price"] = pd.to_numeric(df["price"], errors="coerce")

                # Load data into PostgreSQL
                df.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=1000)

                logging.info(f"Successfully loaded {file_name} into {table_name}.")
            else:
                logging.error(f"File {file_path} not found!")
        except Exception as e:
            logging.error(f"Error loading {file_name}: {e}")

# Define DAG
dag = DAG(
    'etl_pipeline_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
    catchup=False
)

# Define tasks
download_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag
)

etl_task = PythonOperator(
    task_id="extract_and_load",
    python_callable=extract_and_load,
    dag=dag
)

# Set dependencies
download_task >> etl_task
