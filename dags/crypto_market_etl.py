from airflow import DAG
import logging
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
import datetime as datetime
from airflow.sdk import Asset

crypto_dataset = Asset("sqlite:///tmp/crypto.db")


logs = logging.getLogger(__name__)

def on_failure_callback(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    logical_date = context['logical_date']
    logs.error(f"Task {task_id} failed on DAG {dag_id} at {logical_date}")

@task(retries = 2, retry_delay=datetime.timedelta(seconds = 30), on_failure_callback=on_failure_callback)
def extract():
    
    import requests as req
    from airflow.models import Variable
    from airflow.operators.python import get_current_context

    
    crypto_value = Variable.get('crypto_currency',default_var='usd')
    logs.info('Calling API')
    response = req.get(f'https://api.coingecko.com/api/v3/coins/markets?vs_currency={crypto_value}&order=market_cap_desc&per_page=10&page=1')  
    if response.status_code != 200:
        raise Exception(f"API returned status {response.status_code}") 
    logs.info('API Called Successfull')
    data = response.json()
    
    context = get_current_context()
    logical_date = context['logical_date'].isoformat()
    
    for record in data:
        record['extracted_at'] = logical_date
    return data
    

@task
def transform(data):
    
    import pandas as pd
    import math 
    
    logs.info('Transforming data')
    needed_columns = ['id', 'symbol', 'current_price', 'market_cap', 'price_change_percentage_24h', 'extracted_at']
    missing = set(needed_columns) - set(data[0].keys())
    if missing:
        raise Exception(f"Schema changed, Missing Columns: {missing}")
    
    
    needed_keys = ['id', 'symbol', 'current_price', 'market_cap', 'price_change_percentage_24h', 'extracted_at']
    dt = [{k: r[k] for k in needed_keys} for r in data]
    
    for records in dt:
        
        val = records['price_change_percentage_24h']
        if isinstance(val,float) and math.isnan(val):
            records['price_change_percentage_24h'] = None
        
        market_cap = records['market_cap']
        if market_cap > 1e10:
            records['market_cap_tier'] = 'large_cap'
        elif market_cap > 1e9:
            records['market_cap_tier'] = 'mid_cap'
        else:
            records['market_cap_tier'] = 'small_cap'
            
    return dt

@task
def load(data):
    
    import sqlite3 as sqlite
    from airflow.operators.python import get_current_context
    from airflow.models import Variable
    
    context = get_current_context()
    run_id = context['ti'].run_id.replace(':','').replace('+','').replace('-','')
    
    database_file_path = Variable.get('database_file_path', default_var='/tmp/crypto.db')
    
    logs.info("Creating sqlite connection")
    conn = sqlite.connect(database_file_path)
    logs.info("Connection setup completed")
    
    try:
        cursor = conn.cursor()
        sql_script = """
            CREATE TABLE IF NOT EXISTS CRYPTO_SNAPSHOTS(
                id varchar,
                run_id varchar,
                symbol varchar,
                current_price decimal, 
                market_cap decimal,
                market_cap_tier varchar,
                price_change_percentage_24h decimal, 
                extracted_at text,
                UNIQUE(id, run_id)
            )      
        """
        logs.info("creating table if not exists")
        cursor.execute(sql_script)
        
        
        logs.info("Insert into table")
        for records in data:
            cursor.execute(
                "INSERT OR IGNORE INTO crypto_snapshots VALUES (?,?,?,?,?,?,?,?)",
                (records['id'], run_id, records['symbol'], records['current_price'],records['market_cap'], records['market_cap_tier'], records['price_change_percentage_24h'], records['extracted_at'])
            )
                
        conn.commit()
    finally:
        conn.close()
        
@task(outlets=[crypto_dataset])
def quality_check():
    import sqlite3 as sqlite
    from airflow.models import Variable
    from airflow.operators.python import get_current_context
    
    
    database_file_path = Variable.get('database_file_path', default_var='/tmp/crypto.db')
    conn = sqlite.connect(database_file_path)
    
    try:
        
        cursor = conn.cursor()
        
        run_id = get_current_context()['ti'].run_id.replace(':','').replace('+','').replace('-','')
        
        cnt_run_id = cursor.execute("SELECT COUNT(*) FROM crypto_snapshots WHERE run_id = ?", (run_id,)).fetchone()[0]
        
        if cnt_run_id != 10:
            raise Exception(f"Data quality check failed, only {cnt_run_id} records inserted for run_id {run_id}")
        
        cnt_null_current_price = cursor.execute("SELECT COUNT(*) FROM crypto_snapshots WHERE run_id = ? AND current_price IS NULL", (run_id,)).fetchone()[0]
        
        if cnt_null_current_price > 0:
            raise Exception(f"Data quality check failed, {cnt_null_current_price} records have null current_price for run_id {run_id}")
        
        cnt_exceptions_category = cursor.execute("SELECT COUNT(*) FROM crypto_snapshots WHERE run_id = ? AND market_cap_tier NOT IN ('large_cap', 'mid_cap', 'small_cap')", (run_id,)).fetchone()[0]
        
        if cnt_exceptions_category > 0:
            raise Exception(f"Data quality check failed, {cnt_exceptions_category} records have invalid market_cap_tier for run_id {run_id}")
    finally:
        conn.close()
    
    
    
with DAG(
    dag_id = 'task_dag',
    start_date = datetime.datetime(2026,4,12),
    schedule = None,
    catchup = False 
) as dag:
    
    archive = BashOperator(
        task_id = 'archive',
        bash_command = 'echo "Pipeline complete"'
    )
    
    wait_for_file = FileSensor(
        task_id = 'wait_for_file',
        filepath = '/tmp/run_trigger.txt',
        mode = 'poke',
        poke_interval = 30,
        timeout = 300
    )
    
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    loaded_data = load(transformed_data)
    quality_check_result = quality_check()
    wait_for_file >> extracted_data >> transformed_data >> loaded_data >> quality_check_result >> archive

