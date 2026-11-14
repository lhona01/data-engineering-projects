import pandas as pd
import logging
import os
import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)

logger = logging.getLogger(__name__) # messages show up in Airflow's task logs

def load_config():
    with open(os.path.join(SCRIPT_DIR, "config.yaml")) as f:
        return yaml.safe_load(f) 

# Extract
def extract_data(file_path):
    df = pd.read_csv(file_path)
    logger.info("Data extracted successfully!")
    return df

# Clean
def clean_data(df):
    df = df.drop_duplicates()
    df['customer_name'] = df['customer_name'].fillna('Unknown')
    df['amount'] = df['amount'].fillna(0)
    logger.info("Data cleaned successfully!")
    return df

# Validate (ensure data is in the correct format)
def validate_data(df):
    # amount in range
    accepted_df = df[df["amount"] >= 0]
    rejected_df = df[df["amount"] < 0]

    # date is valid
    parsed = pd.to_datetime(accepted_df['order_date'], errors='coerce')
    bad_mask = parsed.isna()

    rejected_df = pd.concat([rejected_df, accepted_df[bad_mask]], ignore_index=True)
    accepted_df = accepted_df[~bad_mask].copy()
    accepted_df['order_date'] = parsed[~bad_mask]

    logger.info("Data validated successfully!")
    return accepted_df, rejected_df

# Load
def load_data(df, file_path):
    df.to_csv(file_path, index=False)
    logger.info("Data saved successfully")

# run pipeline
def run_pipeline():
    config = load_config()

    logging.basicConfig(
        filename=os.path.abspath(os.path.join(BASE_DIR, config['logging']['file'])),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    
    df = extract_data(os.path.join(BASE_DIR, config['source']['raw']))
    df = clean_data(df)
    accepted_df, rejected_df = validate_data(df)
    load_data(accepted_df,os.path.join(BASE_DIR, config['destination']['clean']))
    load_data(rejected_df, os.path.join(BASE_DIR, config['destination']['quarantine']))
    logging.info("Cleaning pipeline saved successfully")

if __name__ == "__main__":
    run_pipeline()