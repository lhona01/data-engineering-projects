import pandas as pd
from sqlalchemy import create_engine, text
import requests
import logging
from dotenv import load_dotenv
import os
import yaml

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # /..../project_2_api_pipeline/scripts
BASE_DIR = os.path.dirname(SCRIPT_DIR) # /..../project_2_api_pipeline

# set up logging
def load_config():
    with open(os.path.join(SCRIPT_DIR, "config.yaml")) as f:
        return yaml.safe_load(f) # conver config.yaml to python dictionary

config = load_config()

logging.basicConfig(
    filename=os.path.join(BASE_DIR, config["logging"]["file"]),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# database connection
engine = create_engine(os.getenv("DATABASE_URL"))

# Ingest data from API
def extract_data():
    url = config["source"]["api_url"]

    response = requests.get(url)
    if response.status_code == 200:
        logging.info("API data fetched successfully!")
        return response.json()
    else:
        logging.error("Failed to fetch API data")
        raise Exception("API request failed")

# -----------------------API response validation---------------------------------
# validate api response keys/json structure
def validate_json_structure(data):
    current = None
    if "current_weather" in data:
        current = data['current_weather']
    else:
        logging.error("Failed to validate api resposne key")
        raise Exception("JSON Structure changed!")

    api_response_key = ['temperature', 'windspeed', 'winddirection', 'time']

    for key in api_response_key:
        if key in current:
            continue
        else:
            logging.error("Failed to validate api response key")
            raise Exception("JSON Structure changed!")

# validate duplicate record- if weather data hasn't been changed/updated, don't load it to table again
# Incremental Loading??
def duplicate_record_in_table(df):
    with engine.connect() as conn:
        latest = conn.execute(text(
            """
            SELECT observation_time
            FROM weather_data
            ORDER BY observation_time DESC
            LIMIT 1
            """)).scalar()
    
    if latest is not None and pd.to_datetime(df['current_weather']['time']) == pd.Timestamp(latest):
        logging.error("Failed to load data, duplicate record!")
        return True
    return False
#------------------------------------------------------------------------------

# Transform JSON data recieved via API call into pandas table (done)
def transform_data(data):
    current = data['current_weather']

    df = pd.DataFrame([{
        'temperature': current['temperature'],
        'windspeed': current['windspeed'],
        'winddirection': current['winddirection'],
        'observation_time': pd.to_datetime(current['time'])
    }])

    logging.info("Data transformed successfully!")
    return df

# Load the data into PostgreSQL database
def load_data(df):
    df.to_sql(config["destination"]["table_name"], engine, if_exists=config["destination"]["if_exists"], index=False)
    logging.info("Data loaded successfully!")

# Run API pipeline
def run_pipeline():
    data = extract_data()
    validate_json_structure(data)
    if not duplicate_record_in_table(data):
        data = transform_data(data)
        load_data(data)
        logging.info("Pipeline executed successfully!")

if __name__ == "__main__":
    run_pipeline()