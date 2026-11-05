# Project 2: Weather API → Postgres ETL with Airflow

A pipeline that fetches daily weather data from the Open-Meteo API and loads it into a PostgreSQL table, orchestrated by Airflow running in Docker.

**Features**
- Retries failed API calls with exponential backoff and jitter
- Validates incoming data format
- Prevents duplicate records from being loaded
- Transforms data to match the target table schema
- Scheduled and triggered by Airflow (Docker)
- Email notification when piepline fails

## Architecture
API -> pandas validate -> pandas transform -> PostgresSQL -> Airflow (Docker) triggers the run daily -> pipeline failure alerting

## Tech stack
python, pandas, requests, SQLAlchemy, PostgresSQL, APache Airflow, Docker

## Prerequisites
- Python 3.13
- Docker Desktop
- PostgreSQL running locally with a database named 
'postgres'

## Setup
1. Clone the repo and go to the project folder
```bash
  git clone https://github.com/lhona01/data-engineering-projects.git
  cd data-engineering-projects/project_2_api_pipeline
```
2. Create your `.env` from the example and fill in your database credentials:
```bash
  cp .env.example .env
```
3. Create a virtual environment and install dependencies:
```bash
  python -m venv venv
  venv\Scripts\activate
  pip install -r requirements.txt
```
4. Run the pipeline
```bash
  python scripts/api_pipeline.py
```
## Running with Airflow
1. Go to the `airflow` folder at the repo root and copy `.env.example` to `.env`
2. `docker compose up`
3. Open the AIrflow UI, create the Postgres connection under Admin -> Connections and use my_postgres_conn_api as DB_CONN_ID, create another generic connection to send pipeline failure alerts to specified email (key:smtp_alerts, host:smtp.gmail.com, port:587), lastly add email to send notification to through Admin -> Variables (key: alert_email_to, value: email address, Is Encrypted: true) and trigger the `project_2_api_pipeline` DAG