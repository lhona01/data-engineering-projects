import pandas as pd
from sqlalchemy import create_engine, text, inspect
import requests
import logging
from dotenv import load_dotenv
import os
import yaml
import random
import time
from requests.exceptions import ConnectionError, Timeout, HTTPError

logger = logging.getLogger(__name__) # messages show up in Airflow's task logs

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # /..../project_2_api_pipeline/scripts
BASE_DIR = os.path.dirname(SCRIPT_DIR) # /..../project_2_api_pipeline

 
class StaleDataError(Exception):
    """Raised when the destination table has no recent rows."""

# set up logging
def load_config():
    with open(os.path.join(SCRIPT_DIR, "config.yaml")) as f:
        return yaml.safe_load(f) # conver config.yaml to python dictionary

# Ingest data from API
def extract_data_with_retry(url, max_retries=5, base_delay=1, max_delay=30, timeout=10):
    RETRYABLE_STATUS = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)

            if response.status_code in RETRYABLE_STATUS:
                raise HTTPError(f"Retryable status {response.status_code}", response=response)

            response.raise_for_status() # non-retryable 4xx errors raise here (only raises error if its a error code)
            logger.info("API data fetched successfully!")
            return response.json()
        except (ConnectionError, Timeout) as e:
            error = e
        except HTTPError as e:
            if e.response is not None and e.response.status_code not in RETRYABLE_STATUS:
                logger.error("Failed to fetch API data")
                raise
            error = e

        # retry logic
        if attempt == max_retries:
            logger.error("Failed to fetch API data")
            raise RuntimeError(f"Failed after {max_retries} attempts") from error

        # Exponential backoff: 1s, 2s, 4s, 8s.... capped at max_delay of 30sec
        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
        delay += random.uniform(0, delay * 0.1) # adding jitter so all api calls aren't occuring at the same time

        # Use Retry-After header if the server sends one
        response = getattr(error, "response", None)
        retry_after = response.headers.get("Retry-After") if response is not None else None
        if retry_after and retry_after.is_digit():
            delay = max(delay, int(retry_after))

        print(f"Attempt {attempt} failed ({error}). Retrying in {delay:.1f}s...")
        time.sleep(delay) 

# -----------------------API response validation---------------------------------
# validate api response keys/json structure
def validate_json_structure(data):
    current = None
    if "current_weather" in data:
        current = data['current_weather']
    else:
        logger.error("Failed to validate api resposne key")
        raise Exception("JSON Structure changed!")

    api_response_key = ['temperature', 'windspeed', 'winddirection', 'time']

    for key in api_response_key:
        if key in current:
            continue
        else:
            logger.error("Failed to validate api response key")
            raise Exception("JSON Structure changed!")

# validate duplicate record- if weather data hasn't been changed/updated, don't load it to table again
# Incremental Loading??
def duplicate_record_in_table(data, engine, table_name):
    if not inspect(engine).has_table(table_name): # ensure table exists first before trying to query it for duplicate check
        return False
    with engine.connect() as conn:
        latest = conn.execute(text(
            """
            SELECT observation_time
            FROM weather_data
            ORDER BY observation_time DESC
            LIMIT 1
            """)).scalar()
    
    if latest is not None and pd.to_datetime(data['current_weather']['time']) == pd.Timestamp(latest):
        logger.info("Duplicate record, skipping load")
        return True
    return False

# check the destination table actually has recent data
def check_freshness(engine, table_name, max_age_days=2):
    with engine.connect() as conn:
        count = conn.execute(text(
            f"SELECT COUNT(*) FROM {table_name} "
            "WHERE observation_time > (NOW() AT TIME ZONE 'UTC') "
            "- make_interval(days => :days)"
        ), {"days": max_age_days}).scalar()
 
    if count == 0:
        raise StaleDataError(f"No rows in {table_name} from the last {max_age_days} days")
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

    logger.info("Data transformed successfully!")
    return df

# Load the data into PostgreSQL database
def load_data(df, table_name, engine, if_exists):
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    logger.info("Data loaded successfully!")

# Run API pipeline
def run_pipeline():
    load_dotenv()
    config = load_config()
    engine = create_engine(os.getenv("DATABASE_URL"))
    logging.basicConfig(
            filename=os.path.join(BASE_DIR, config["logging"]["file"]),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
    
    data = extract_data_with_retry(config['source']['api_url'])
    validate_json_structure(data)
    if not duplicate_record_in_table(data, engine, config['destination']['table_name']):
        data = transform_data(data)
        load_data(data, config['destination']['table_name'], engine, config['destination']['if_exists'])
        logger.info("Pipeline executed successfully!")

if __name__ == "__main__":
    run_pipeline()