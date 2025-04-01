from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime

import requests



def print_welcome():

    print('Welcome to Airflow!')



def print_date():

    print('Today is {}'.format(datetime.today().date()))



def print_random_quote():

    response = requests.get('https://dummyjson.com/quotes/1')

    data = response.json()
    quote = data.get('quote', 'No quote found')  # Handle missing data
    author = data.get('author', 'Unknown')  # Fetch author if available
    print(f'Quote of the day: "{quote}" - {author}')




dag = DAG(

    'welcome_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)


# DAG to print the welcome message
print_welcome_task = PythonOperator(

    task_id='print_welcome',

    python_callable=print_welcome,

    dag=dag

)


# DAG to print the current date
print_date_task = PythonOperator(

    task_id='print_date',

    python_callable=print_date,

    dag=dag

)


# DAG to print the random quote
print_random_quote = PythonOperator(

    task_id='print_random_quote',

    python_callable=print_random_quote,

    dag=dag

)



# Set the dependencies between the tasks: 
print_welcome_task >> print_date_task >> print_random_quote

#1st print_welcome_task  will execute
#2nd print_date_task will execute
#3RD print_random_quote will execute



