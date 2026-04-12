from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id = 'dbt_dag',
    start_date = datetime(2026,4,2),
    schedule = '0 6 * * *',
    catchup = False
) as dag:
    task1 = BashOperator(
        task_id = 'dbt_run',
        bash_command = 'dbt run  --project-dir /usr/local/airflow/include/e_commerce --profiles-dir  /usr/local/airflow/include/e_commerce'
    )
    
    task2 = BashOperator(
        task_id = 'dbt_test',
        bash_command = 'dbt test  --profiles-dir /usr/local/airflow/include/e_commerce --project-dir /usr/local/airflow/include/e_commerce'
    )

    task1 >> task2
