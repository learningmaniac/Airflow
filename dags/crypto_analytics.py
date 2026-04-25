from airflow.decorators import task
from airflow import DAG
from airflow.sdk import Asset
import datetime
import logging 

logs = logging.getLogger(__name__)
crypto_dataset = Asset("sqlite:///tmp/crypto.db")

@task
def crypto_aggregate():
    
    import sqlite3 as sqlite
    from airflow.models import Variable
    
    database_file_path = Variable.get('database_file_path', default_var='/tmp/crypto.db')   
    
    conn = sqlite.connect(database_file_path)
    
    try:
        cursor = conn.cursor()
        max_run_id = cursor.execute("SELECT run_id FROM crypto_snapshots ORDER BY extracted_at DESC LIMIT 1").fetchone()[0]
        avg_current_price = cursor.execute("SELECT AVG(current_price) FROM crypto_snapshots where run_id = ?", (max_run_id,)).fetchone()[0]
        total_market_cap = cursor.execute("SELECT SUM(market_cap) FROM crypto_snapshots where run_id = ?", (max_run_id,)).fetchone()[0]
        count_market_cap_tier = cursor.execute("SELECT market_cap_tier, COUNT(*) FROM crypto_snapshots where run_id = ? GROUP BY market_cap_tier", (max_run_id,)).fetchall()
        logs.info(f"Average current price: {avg_current_price}")
        logs.info(f"Total market cap: {total_market_cap}")
        logs.info(f"Count by market cap tier: {count_market_cap_tier}")
    finally:
        conn.close()
        
with DAG (
    dag_id = 'crypto_analytics',
    start_date = datetime.datetime(2026,4,23),
    schedule = [crypto_dataset],
    catchup = False
) as dag:
    
    crypto_aggregate_results = crypto_aggregate()
    
    