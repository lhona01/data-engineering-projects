import pandas as pd
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from data_validation import validate_data, load_rules
import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)

def load_config():
    with open(os.path.join(SCRIPT_DIR, "config.yaml")) as f:
        return yaml.safe_load(f)

def extract_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def transform_data(data):
    orders_list = []
    items_list = []

    for order in data:
        orders_list.append({
            "order_id": order["order_id"],
            "customer_name": order["customer_name"],
            "order_date": pd.to_datetime(order["order_date"])
            })

        for item in order["items"]:
            items_list.append({
                "order_id": order["order_id"],
                "product": item["product"],
                "price": item["price"]
            })

    orders_df = pd.DataFrame(orders_list)
    items_df = pd.DataFrame(items_list)

    return orders_df, items_df

def load_data(orders_df, items_df, config):
    engine = create_engine(os.getenv("DATABASE_URL"))
    orders_df.to_sql(config["destination"]["orders"], engine, if_exists='append', index=False)
    items_df.to_sql(config["destination"]["items"], engine, if_exists="append", index=False)

def run_pipeline():
    load_dotenv()
    config = load_config()
    data = extract_data(os.path.join(BASE_DIR, config["source"]["json_path"]))
    orders_df, items_df = transform_data(data)

    rules = load_rules(os.path.join(SCRIPT_DIR, config["validation"]["orders_rule"]))
    accepted_orders, rejected_orders = validate_data(orders_df, rules)
    rejected_orders.to_csv(config["destination"]["orders_quarantine"])

    rules = load_rules(os.path.join(SCRIPT_DIR, config["validation"]["items_rule"]))
    accepted_items, rejected_items = validate_data(items_df, rules)
    rejected_items.to_csv(config["destination"]["items_quarantine"])

    load_data(accepted_orders, accepted_items, config)
    print("JSON pipeline executed successfully")

if __name__ == "__main__":
    run_pipeline()