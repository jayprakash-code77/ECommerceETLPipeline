from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import kagglehub
import requests

# Download latest version
def printpath():
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    print("Path to dataset files:", path)


dag = DAG(
    'dataDown_DAG',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False
)


print_path_task = PythonOperator(
    task_id="printpath",

    python_callable=printpath,

    dag=dag
)

print_path_task
