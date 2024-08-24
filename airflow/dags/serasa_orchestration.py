from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import subprocess

def run_pyspark_script(script_path):
    subprocess.run(['python', script_path], check=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'pyspark_workflow2',
    default_args=default_args,
    description='DAG to orchestrate PySpark jobs',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    run_raw_to_silver = BashOperator(
    task_id='run_raw_to_silver', 
    bash_command='python3 /opt/airflow/source/lake/raw_to_silver.py', 
    )

    run_silver_to_gold = BashOperator(
    task_id='run_silver_to_gold', 
    bash_command='python3 /opt/airflow/source/lake/silver_to_gold.py', 
    )
    
    run_raw_to_silver >> run_silver_to_gold
