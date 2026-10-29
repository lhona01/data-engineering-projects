import pandas as pd
from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import os
import yaml

logger = logging.getLogger(__name__) # messages show up in Airflow's task logs

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # .../project_1_csv_pipeline/scripts
BASE_DIR = os.path.dirname(SCRIPT_DIR) # .../project_1_csv_pipeline

def load_config():
    with open(os.path.join(SCRIPT_DIR, "config.yaml")) as f:
        return yaml.safe_load(f) # return values in config.yaml as python dictionary

# Extract (ingest data from csv file)
def extract_data(file_path):
    df = pd.read_csv(file_path)
    logger.info("Data extracted successfully")
    return df

# Transform (clean the data)
def transform_data(df):
    df = df.drop_duplicates()
    df['amount'] = df['amount'].fillna(0)
    df['order_date'] = pd.to_datetime(df['order_date'])
    logger.info("Data transformed successfully")
    return df

# validate date before loading 
def validate_data(df):
    errors = []

    if df["order_date"].isna().any():
        errors.append("order_date has missing value")
    if (df["amount"] < 0).any():
        errors.append("amount has negetive values")
    if df.duplicated().any():
        errors.append("duplicate rows found")
    if df.empty:
        errors.append("dataframe is empty")

    if errors:
        raise ValueError("Validation failed: " + "; ".join(errors))

    logger.info("Data validation passed")
    return df

# Load (Load transformed data into sales table)
def load_data(df, engine, table_name, if_exists):
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    logger.info("Data loaded successfully")

# Run ETL pipeline
def run_pipeline():
    """Standalone run: python csv_pipeline (no Airflow needed)"""
    load_dotenv()
    config = load_config()
    logging.basicConfig(
        filename=os.path.join(BASE_DIR, config["logging"]["file"]),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    df = extract_data(os.path.join(BASE_DIR, config["source"]["csv_path"]))
    df = transform_data(df)
    df = validate_data(df)
    engine = create_engine(os.getenv("DATABASE_URL"))
    load_data(df, engine, config["destination"]["table_name"],
              config["destination"]["if_exists"])
    logger.info("Pipeline executed successfully")

if __name__ == "__main__":
    run_pipeline()