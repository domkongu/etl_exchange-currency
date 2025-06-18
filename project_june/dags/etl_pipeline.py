from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='exchange_rates_etl',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_data = BashOperator(
        task_id='fetch_exchange_rates',
        bash_command='python /opt/airflow/scripts/fetch_exchange_rates.py'
    )

    load_data = BashOperator(
        task_id='load_to_postgres',
        bash_command='python /opt/airflow/scripts/load_to_postgres.py'
    )

    fetch_data >> load_data 