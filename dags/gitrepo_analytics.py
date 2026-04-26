from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset
import datetime as datetime

@task
def analytics():
    pass

github_trending_dataset = Asset("sqlite:///tmp/github_trending.db")

with DAG(
    dag_id = 'gitrepo_analytics',
    start_date = datetime.datetime(2024, 6, 1),
    schedule = [github_trending_dataset],
    catchup = False
) as dag:
    
    create_analytics_table = analytics()