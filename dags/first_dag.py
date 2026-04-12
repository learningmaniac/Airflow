from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracti data...")

def load():
    print("Loading data...")
    
with DAG(
    dag_id='first_dag',
    start_date=datetime(2024, 6, 1),
    schedule='0 6 * * *',
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id = 'extract_task',
        python_callable = extract
    )
    
    task2 = PythonOperator(
        task_id = 'load_task',
        python_callable = load
    )   
    
    task1 >> task2