from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import sys
import pandas as pd

# Update Python path to include your module location
sys.path.insert(0, os.path.abspath(r"C:Users/back2/Desktop/DESORTED_FILEZ/Airflow_automation/dags/modules"))

# Import functions
from modules.Extract import Extract_csv_data
from modules.Transform import transform_data
from modules.load import create_tables
from modules.helpers import get_container_client, data_loading

# Shared data store (replace with XCom in real-world DAGs)
shared_data = {}

# Task Wrappers
def extract_task_callable(**kwargs):
    df = Extract_csv_data('./dags/ziko_logistics_data.csv')
    shared_data['df'] = df

def transform_task_callable(**kwargs):
    df = shared_data.get('df')
    transformed_df = transform_data(df)
    shared_data['df'] = transformed_df

def create_tables_task_callable(**kwargs):
    df = shared_data.get('df')
    customer_df, product_df, transaction_df = create_tables(df)
    shared_data['customer_df'] = customer_df
    shared_data['product_df'] = product_df
    shared_data['transaction_df'] = transaction_df

def load_to_azure_task_callable(**kwargs):
    container_client = get_container_client()
    data_loading(
        shared_data['customer_df'],
        shared_data['product_df'],
        shared_data['transaction_df'],
        container_client
    )

# Default arguments
default_args = {
    'owner': 'Gold_finteck',
    'depends_on_past': False,
    'email': ['ifigeorgeifi@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='ziko_logistic_pipeline',
    default_args=default_args,
    description='Pipeline to fetch, transform, and load data into Azure storage',
    schedule_interval='@monthly',
    start_date=datetime(2025, 6,28 ),
    catchup=False,
    tags=['ziko', 'azure', 'ETL']
) as dag:

    start_task = EmptyOperator(task_id='start_pipeline')

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task_callable,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task_callable,
    )

    create_tables_task = PythonOperator(
        task_id='create_dim_fact_tables',
        python_callable=create_tables_task_callable,
    )

    load_data_to_azure = PythonOperator(
        task_id='load_data_to_azure',
        python_callable=load_to_azure_task_callable,
    )

    end_task = EmptyOperator(task_id='end_pipeline')

    # Set task dependencies
    start_task >> extract_data >> transform_data_task >> create_tables_task >> load_data_to_azure >> end_task
