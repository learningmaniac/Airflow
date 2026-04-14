from airflow import DAG
import pandas as pd
import requests as req
import logging
import datetime as datetime
import math 
from airflow.decorators import task

logs = logging.getLogger(__name__)

def on_failure_callback(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    logs.error(f"Task {task_id} failed on DAG {dag_id} at {execution_date}")

@task(retries = 2, retry_delay=datetime.timedelta(seconds = 30), on_failure_callback=on_failure_callback)
def extract():
    logs.info('Calling API')
    response = req.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1')  
    if response.status_code != 200:
        raise Exception(f"API returned status {response.status_code}") 
    logs.info('API Called Successfull')
    data = response.json()
    extracted_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for record in data:
        record['extracted_at'] = extracted_at
    return data
    

@task
def transform(data):
    
    logs.info('Transforming data')
    
    df = pd.DataFrame(data)
    
    df = df[['id', 'symbol', 'current_price', 'market_cap', 'price_change_percentage_24h','extracted_at']]
        
    dt = df.to_dict(orient='records')
    
    for records in dt:
        val = records['price_change_percentage_24h']
        if isinstance(val,float) and math.isnan(val):
            records['price_change_percentage_24h'] = None
    
    return dt

@task
def load(data):
    logs.info('Writing to csv file')
    df = pd.DataFrame(data)
    df.to_csv(f'/tmp/crypto_snapshot.{datetime.date}.csv',index=False)
    
with DAG(
    dag_id = 'task_dag',
    start_date = datetime.datetime(2026,4,12),
    schedule = None,
    catchup = False 
) as dag:
    load(transform(extract()))

