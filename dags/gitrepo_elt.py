from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Asset
import datetime as datetime
import logging

github_trending_dataset = Asset("sqlite:///tmp/github_trending.db")

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
    
    github_min_stars = Variable.get('github_min_stars', default_var='1000')
    git_url = f'https://api.github.com/search/repositories?q=stars:%3E{github_min_stars}&sort=stars&order=desc&per_page=10'
    response = req.get(git_url)
    
    if response.status_code != 200:
        raise Exception(f"API returned status {response.status_code}")
    
    data = response.json()
    
    logical_date = get_current_context()['logical_date'].isoformat()
    
    for record in data['items']:
        record['extracted_at'] = logical_date
        
    data = data['items']
    
    return data

@task
def transform(data):
    # schema validation
    expected_columns = ['id', 'name', 'full_name','forks_count','license', 'description', 'stargazers_count', 'language','owner', 'extracted_at']
    
    missing_columns = set(expected_columns) - set(data[0].keys())
    
    if missing_columns:
        raise Exception(f"Schema changed, Missing Columns: {missing_columns}")
 
    # enriching data
    for record in data:
        record['owner'] = record['owner']['login']
        record['license'] = record['license']['name'] if record.get('license')  else None
    
    # filtering only expected columns
    filtered_data = [{k: r[k] for k in expected_columns} for r in data]
    
    # handling null values
    for record in filtered_data:
        if record['description'] is None:
            record['description'] = 'No description provided'
            
    for record in filtered_data:
        if record['stargazers_count'] > 1e5:
            record['popularity_tier'] =  'mega'
        elif record['stargazers_count'] > 5 * 1e4:
            record['popularity_tier'] = 'high'
        elif record['stargazers_count'] > 1e4:
            record['popularity_tier'] = 'medium'
        else:
            record['popularity_tier'] = 'low'

    return filtered_data


@task
def load(data):
    
    logs.info(f"Load received {len(data)} records")
    import sqlite3 as sqlite
    from airflow.operators.python import get_current_context
    from airflow.models import Variable
    
    logs.info("About to read Variable")
    database_file_path = Variable.get('database_file_path', default_var='/tmp/git_repos.db')
    logs.info(f"Database path: {database_file_path}")
    
    logs.info("About to connect to sqlite")
    conn = sqlite.connect(database_file_path)
    logs.info("Connected to sqlite")
    
    
    try:
        cursor = conn.cursor()
        
        
        run_id = get_current_context()['task_instance'].run_id.replace(':','').replace('+','').replace('-','')
        
        logs.info("Creating table if not exists")
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS github_trending (
                id INTEGER,
                run_id TEXT,
                name TEXT,
                full_name TEXT,
                forks_count INTEGER,
                license TEXT,
                description TEXT,
                stargazers_count INTEGER,
                language TEXT,
                owner TEXT,
                extracted_at TEXT,
                popularity_tier TEXT,
                UNIQUE(run_id, id)
            )
        '''
        cursor.execute(create_table_query)
        
        logs.info("Inserting records into table")
        insert_query = '''
            INSERT OR IGNORE INTO github_trending (run_id, id, name, full_name, forks_count, license, description, stargazers_count, language, owner, extracted_at, popularity_tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            cursor.execute(insert_query, (
                run_id,
                record['id'],
                record['name'],
                record['full_name'],
                record['forks_count'],
                record['license'],
                record['description'],
                record['stargazers_count'],
                record['language'],
                record['owner'],
                record['extracted_at'],
                record['popularity_tier']
            ))
            
        logs.info("Committing transaction")
        conn.commit()
        
    finally:
        logs.info("Closing connection")
        cursor.close()
        logs.info("Connection closed")
        conn.close()

@task(outlets=[github_trending_dataset])
def quality_check():
    
    import sqlite3 as sqlite
    from airflow.operators.python import get_current_context
    from airflow.models import Variable

    database_file_path = Variable.get('database_file_path', default_var='/tmp/git_repos.db')
    conn = sqlite.connect(database_file_path)
    
    try:
        
        cursor = conn.cursor()
        run_id = get_current_context()['task_instance'].run_id.replace(':','').replace('+','').replace('-','')
        
        count_of_records_query = cursor.execute('SELECT COUNT(*) FROM github_trending where run_id = ?', (run_id,))
        count_of_records = cursor.fetchone()[0]
        
        count_of_null_in_id_name_stargazers_query = cursor.execute('SELECT COUNT(*) FROM github_trending WHERE run_id = ? AND (id IS NULL OR name IS NULL OR stargazers_count IS NULL)', (run_id,))
        count_of_null_in_id_name_stargazers = cursor.fetchone()[0]
        
        count_of_invalid_popularity_tier_query = cursor.execute("SELECT COUNT(*) FROM github_trending WHERE run_id = ? AND popularity_tier NOT IN ('mega', 'high', 'medium', 'low')", (run_id,))
        count_of_invalid_popularity_tier = cursor.fetchone()[0]

        if count_of_records != 10:
            raise Exception(f"Expected 10 records, got {count_of_records}")
        if count_of_null_in_id_name_stargazers > 0:
            raise Exception(f"Found {count_of_null_in_id_name_stargazers} rows with null required fields")
        if count_of_invalid_popularity_tier > 0:
            raise Exception(f"Found {count_of_invalid_popularity_tier} rows with invalid popularity_tier")

    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id = "gitrepo_elt",
    start_date = datetime.datetime(2026, 4, 1),
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

