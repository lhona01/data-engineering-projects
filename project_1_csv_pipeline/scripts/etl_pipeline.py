import pandas as pd
from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import os
import yaml

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)

def load_config():
    with open(os.path.join(SCRIPT_DIR, "config.yaml")) as f:
        return yaml.safe_load(f)

config = load_config()

# set up logging
# logging lets you track what the pipeine is doing at every stage- and helps you debug quickly
logging.basicConfig(
    filename=os.path.join(BASE_DIR, config["logging"]["file"]),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Extract (ingest data from csv file)
def extract_data(file_path):
    df = pd.read_csv(file_path)
    logging.info("Data extracted successfully")
    return df

# Transform (clean the data)
def transform_data(df):
    df = df.drop_duplicates()
    df['amount'] = df['amount'].fillna(0)
    df['order_date'] = pd.to_datetime(df['order_date'])
    logging.info("Data transformed successfully")
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

    logging.info("Data validation passed")
    return df

# Load (Load transformed data into sales table)
def load_data(df):
    engine = create_engine(os.getenv("DATABASE_URL"))
    df.to_sql(config["destination"]["table_name"], engine, if_exists=config["destination"]["if_exists"], index=False)
    logging.info("Data loaded successfully")

# Run ETL pipeline
def run_pipeline():
    file_path = os.path.join(BASE_DIR, config["source"]["csv_path"])
    df = extract_data(file_path)
    df = transform_data(df)
    df = validate_data(df)
    load_data(df)
    logging.info("Pipeline executed successfully")

if __name__ == "__main__":
    run_pipeline()