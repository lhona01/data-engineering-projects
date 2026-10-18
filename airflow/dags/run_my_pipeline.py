from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Standard arguments for DAG
default_args = {
    'owner': 'Lomus',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# INitialize the DAG context
with DAG(
    dag_id='project_1_csv_pipeline',  # The name that will show up in the UI
    default_args=default_args,
    description='Runs Project 1 ETL script from my book',
    schedule=None,             # "None" means it only runs when you click 'Play'
    start_date=datetime(2026,10,17),    # Airflow needs a historical start date to initialize
    catchup=False,                       # Don't backfill missed runs from the start date
) as dag:

    # Task
    run_etl = BashOperator(
        task_id='execute_etl_pipeline',
        bash_command='python /opt/airflow/scripts/etl_pipeline.py'
    )

    # workflow dependencies (just 1 task)
    run_etl