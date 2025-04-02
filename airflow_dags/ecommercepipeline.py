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

# KaggleHub dataset directory
DATASET_DIR = "/home/airflow/.cache/kagglehub/datasets/olistbr/brazilian-ecommerce/versions/2"
CSV_FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv"
}

def download_dataset():
    """Downloads the dataset from KaggleHub."""
    logging.info("Downloading dataset from KaggleHub...")
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    
    if os.path.exists(path):
        logging.info(f"Dataset downloaded to: {path}")
        logging.info("Files in dataset directory:")
        for root, dirs, files in os.walk(path):
            for name in files:
                logging.info(os.path.join(root, name))
    else:
        logging.error(f"Download path not found: {path}")

def extract_and_load():
    """Extracts data from KaggleHub cache and loads into PostgreSQL."""
    engine = create_engine(DB_URI)
    
    for table_name, file_name in CSV_FILES.items():
        file_path = os.path.join(DATASET_DIR, file_name)
        
        try:
            if os.path.exists(file_path):
                logging.info(f"Loading {file_name} into {table_name} table...")
                
                df = pd.read_csv(file_path)
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                
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












