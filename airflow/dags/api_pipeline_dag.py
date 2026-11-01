import os
import sys
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

PROJECT_DIR = "/opt/airflow/projects/project_2_api_pipeline"
sys.path.insert(0, f"{PROJECT_DIR}/scripts")
import api_pipeline as api_etl

STAGING_DIR = f"{PROJECT_DIR}/data/staging"
RAW = f"{STAGING_DIR}/raw.json"
CLEAN = f"{STAGING_DIR}/clean.parquet"
DB_CONN_ID = "my_postgres_conn_api"

def _read_raw():
  with open(RAW) as f:
    return json.load(f)
  
def extract_and_validate():
  config = api_etl.load_config()
  os.makedirs(STAGING_DIR, exist_ok=True)
  data = api_etl.extract_data_with_retry(config["source"]["api_url"])
  api_etl.validate_json_structure(data)
  with open(RAW, "w") as f:
    json.dump(data, f)

def check_duplicate_record():
  config = api_etl.load_config()
  engine = PostgresHook(postgres_conn_id=DB_CONN_ID).get_sqlalchemy_engine()
  is_dup = api_etl.duplicate_record_in_table(
      _read_raw(), engine, config["destination"]["table_name"]
  )
  return "finish" if is_dup else "transform"

def transform():
  api_etl.transform_data(_read_raw()).to_parquet(CLEAN, index=False)

def load():
  config = api_etl.load_config()
  engine = PostgresHook(postgres_conn_id=DB_CONN_ID).get_sqlalchemy_engine()
  api_etl.load_data(
      pd.read_parquet(CLEAN),
      config["destination"]["table_name"],
      engine,
      if_exists=config["destination"]["if_exists"],
  )


default_args = {
    "owner": "Lomus",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="project_2_api_pipeline",
    default_args=default_args,
    description="project 2: api -> postgres",
    schedule="@daily",
    start_date=datetime(2026, 10, 31),
    catchup=False,
) as dag:

  t_extract = PythonOperator(
      task_id="extract", python_callable=extract_and_validate
  )
  t_duplicate_check = BranchPythonOperator(
      task_id="duplicate_check", python_callable=check_duplicate_record
  )
  t_transform = PythonOperator(task_id="transform", python_callable=transform)
  t_load = PythonOperator(task_id="load", python_callable=load)
  t_finish = EmptyOperator(task_id="finish")

  # Dependencies
  t_extract >> t_duplicate_check
  t_duplicate_check >> [t_finish, t_transform]
  t_transform >> t_load