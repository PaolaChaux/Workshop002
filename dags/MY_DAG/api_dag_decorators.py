from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
import os
import sys

from etl import extract_csv, extract_basedatos, transform_spotify, transform_grammy, merge, load, store


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'workflow_api_etl_dag',
    default_args=default_args,
    description='Spotify and Grammys analysis',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    merge_task = PythonOperator(
        task_id = 'merge_task',
        python_callable = merge,
        provide_context = True,
    )

    extract_csv_task = PythonOperator(
        task_id = 'extract_csv_task',
        python_callable = extract_csv,
        provide_context = True,
    )

    transform_s_task = PythonOperator(
        task_id = 'transform_s_task',
        python_callable = transform_spotify,
        provide_context = True,
    )

    extract_bd_task = PythonOperator(
        task_id = 'extract_bd_task',
        python_callable = extract_basedatos,
        provide_context = True,
    )

    transform_g_task = PythonOperator(
        task_id = 'transform_g_task',
        python_callable = transform_grammy,
        provide_context = True,
    )

    store_task = PythonOperator(
        task_id='store_task',
        python_callable = store,
        provide_context = True,
    )

    load_task = PythonOperator(
        task_id ='load_task',
        python_callable = load,
        provide_context = True,
    )
    
    extract_csv_task >> transform_s_task >> merge_task
    extract_bd_task >> transform_g_task >> merge_task
    merge_task >> load_task >> store_task