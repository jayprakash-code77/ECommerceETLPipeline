from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import kagglehub
import os
import zipfile
import pandas as pd

# Define dataset directory
DATASET_DIR = "/home/airflow/.cache/kagglehub/datasets/olistbr/brazilian-ecommerce/versions/2"
CSV_FILE_PATH = os.path.join(DATASET_DIR, "olist_customers_dataset.csv")

# Function to download dataset
def download_dataset():
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    print(f"Dataset downloaded to: {path}")

    # List files in the downloaded path
    if os.path.exists(path):
        print("Files in dataset directory:")
        for root, dirs, files in os.walk(path):
            for name in files:
                print(os.path.join(root, name))
    else:
        print(f"Download path not found: {path}")

# Function to load dataset into pandas
def load_and_display_data():
    if os.path.exists(CSV_FILE_PATH):
        df = pd.read_csv(CSV_FILE_PATH)
        
        # Check if 'customer_city' column exists
        if 'customer_city' in df.columns:
            print("Cities of first 5 customers:\n", df['customer_city'].head().to_string(index=False))
        else:
            print("Column 'customer_city' not found in the dataset.")
        
    else:
        print("Dataset file not found:", CSV_FILE_PATH)


# Define DAG
dag = DAG(
    'data_download_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

# Define tasks
download_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_and_display_data",
    python_callable=load_and_display_data,
    dag=dag
)

# Set task dependencies
download_task >> load_task
