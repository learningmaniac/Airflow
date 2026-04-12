import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests as req
import logging
import os
log = logging.getLogger(__name__)


def fetch_data(**context):
    log.info("Fetching data from API...")
    response = req.get('https://jsonplaceholder.typicode.com/posts')
    if response.status_code != 200:
        log.error(f"Failed to fetch data. Status code: {response.status_code}")
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    data = response.json()
    df = pd.DataFrame(data)
    dt = df.to_dict(orient='records')
    log.info(f"Fetched {len(dt)} records from API.")
    context['ti'].xcom_push(key='raw_data',value=dt)

def transform_data(**context):
    log.info("Transforming data...")
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='fetch_data')
    df = pd.DataFrame(raw_data)
    df['title_length'] = df['title'].apply(len)
    transformed_data = df.to_dict(orient='records')
    log.info(f"Transformed data with {len(transformed_data)} records.")
    context['ti'].xcom_push(key='transformed_data', value=transformed_data)
    
def load_data(**context):
    log.info("Loading data into Snowflake...")
    transformed_data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    try:
        log.info(f"Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        log.info(f"Successfully connected to Snowflake.")
        log.info(f"Data to load: {len(transformed_data)} records.")
        write_pandas(conn, pd.DataFrame(transformed_data), auto_create_table=False,overwrite=True,table_name='API_POSTS')
        log.info(f"Data loaded successfully into Snowflake.")
    except Exception as e:
        log.error(f"Error occurred while loading data into Snowflake: {e}")
        raise
    finally:
        if conn:
            conn.close()
            log.info("Snowflake connection closed.")
with DAG(
    dag_id='api_dag',
    start_date=datetime(2024, 6, 1),
    schedule='0 6 * * *',
    catchup=False
) as dag:
    

    task1 = PythonOperator(
        task_id = 'fetch_data',
        python_callable = fetch_data
    )
    
    task2 = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_data
    )
    
    task3 = PythonOperator(
        task_id = 'load_data',  
        python_callable = load_data
    )
    
    task1 >> task2 >> task3
    
    
    