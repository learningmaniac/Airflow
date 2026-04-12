from airflow import DAG
import pandas as pd
import requests as req
import logging
import datetime as datetime
from airflow.decorators import task
import time

logs = logging.getLogger(__name__)

@task(retries = 2, retry_delay=datetime.timedelta(seconds = 30))
def extract():
    logs.info('Calling API')
    response = req.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1')  
    if response.status_code != 200:
        raise Exception(f"API returned status {response.status_code}") 
    logs.info('API Called Successfull')
    return response.json()

@task
def transform(data):
    
    logs.info('Transforming data')
    
    df = pd.DataFrame(data)
    
    df = df[['id', 'symbol', 'current_price', 'market_cap', 'price_change_percentage_24h']]
    
    df['extracted_at'] = datetime.datetime.now(datetime.timezone.utc)
    
    dt = df.to_dict(orient='records')
    
    return dt

@task
def load(data):
    logs.info('Writing to csv file')
    df = pd.DataFrame(data)
    df.to_csv('/tmp/crypto_snapshot.csv',index=False)
    
with DAG(
    dag_id = 'task_dag',
    start_date = datetime.datetime(2026,4,12),
    schedule = None,
    catchup = False 
) as dag:
    load(transform(extract()))

