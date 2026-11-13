import pandas as pd
import logging
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)

logging.basicConfig(
    filename=os.path.abspath(os.path.join(base_dir, "logs", "pipeline.log")),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Extract
def extract_data(file_path):
    df = pd.read_csv(file_path)
    logging.info("Data extracted successfully!")
    return df

# Clean
def clean_data(df):
    df.drop_duplicates()
    df['customer_name'] = df['customer_name'].fillna('Unknown')
    df['amount'] = df['amount'].fillna(0)
    logging.info("Data cleaned successfully!")
    return df

# Validate (ensure data is in the correct format)
def validate_data(df):
    df = df[df['amount'] >= 0]
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df = df.dropna(subset=['order_date'])
    logging.info("Data validated successfully!")
    return df

# Load
def load_data(df):
    df.to_csv(os.path.join(base_dir, "data", "clean_data.csv"))
    logging.info("Clean data saved successfully")

# run pipeline
def run_pipeline():
    df = extract_data(os.path.join(base_dir, "data", "raw_data.csv"))
    df = clean_data(df)
    df = validate_data(df)
    load_data(df)
    logging.info("Cleaning pipeline saved successfully")

if __name__ == "__main__":
    run_pipeline()