from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
import sys


# Ensure the scripts directory is in the path
sys.path.append('/opt/airflow/scripts')  

# Import your Spotify ETL script
from spotify_etl import run_spotify_etl

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spotify_etl_dag',
    default_args=default_args,
    description = 'A simple Spotify ETL DAG',
    schedule_interval='@daily',  # Run daily
    #schedule_interval=None,
    catchup=False  # Do not backfill
)


run_etl_task = PythonOperator(
    task_id='spotify_etl_task',
    python_callable=run_spotify_etl,
    dag=dag
)

run_etl_task