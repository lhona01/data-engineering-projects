import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PROJECT_DIR = "/opt/airflow/projects/project_1_csv_pipeline"
sys.path.insert(0, f"{PROJECT_DIR}/scripts")
import csv_pipeline as etl

STAGING_DIR = f"{PROJECT_DIR}/data/staging"
RAW = f"{STAGING_DIR}/raw.parquet"
CLEAN = F"{STAGING_DIR}/clean.parquet"
DB_CONN_ID = "my_postgres_conn"

default_args = {
    "owner": "Lomus",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def extract():
    config = etl.load_config()
    os.makedirs(STAGING_DIR, exist_ok=True)
    df = etl.extract_data(os.path.join(PROJECT_DIR, config["source"]["csv_path"]))
    df.to_parquet(RAW, index=False)

def transform():
    etl.transform_data(pd.read_parquet(RAW)).to_parquet(CLEAN, index=False)

def validate():
    etl.validate_data(pd.read_parquet(CLEAN))

def load():
    config = etl.load_config()
    engine = PostgresHook(postgres_conn_id=DB_CONN_ID).get_sqlalchemy_engine()
    etl.load_data(pd.read_parquet(CLEAN), engine, config["destination"]["table_name"], config["destination"]["if_exists"])

with DAG(
    dag_id="project_1_csv_pipeline",
    default_args=default_args,
    description="Project 1: CSV -> Postgres",
    schedule=None,
    start_date=datetime(2026, 10, 17),
    catchup=False,
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_validate = PythonOperator(task_id="validate", python_callable=validate)
    t_load = PythonOperator(task_id="load", python_callable=load)

    t_extract >> t_transform >> t_validate >> t_load