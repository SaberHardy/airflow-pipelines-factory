from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

from tools_helps.hook_tools import create_table, query_data, insert_data_into_table

# This is a text
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('postgres_hook_example',
         default_args=default_args,
         schedule=None,
         catchup=False) as dag:
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_into_table
    )

    query_data_task = PythonOperator(
        task_id='query_data',
        python_callable=query_data
    )

    create_table_task >> insert_data_task >> query_data_task
