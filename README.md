# Project 1: CSV to Postgres ETL with Airflow
Reads sales data from a CSV, cleans & validates data quality with pandas, and loads it into a PostgresSQL table. Airflow (running in Docker) triggers the pipeline.

## Architecture
CSV -> pandas transform -> pandas validate -> PostgreSQL
Airflow (Docker) triggers the run

## Tech stack
python, pandas, SQLAlchemy, PostgreSQL, Apache Airflow, Docker

## Prerequisites
- Python 3.13
- Docker Desktop
- PostgreSQL running locally with a database named 'data_projects'

## Setup
1. Clone the repo and go to the project folder
```bash
  git clone https://github.com/lhona01/data-engineering-projects.git
  cd data-engineering-projects/project_1_csv_pipeline
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
  python scripts/etl_pipeline.py
```
## Running with Airflow
1. Go to the `airflow` folder at the repo root and copy `.env.example` to `.env`
2. `docker compose up`
3. Open the AIrflow UI, create the Postgres connection under Admin -> Connections, and trigger the `project_1_csv_pipeline` DAG

4. ## What the pipeline does
5. - **Extract:** [reads data/sales_data.csv]
   - **Transform:** [drop duplicates, fill null values for amount, convert order_date to pandas datetime]
   - **Load:** [to sales table, perform append]
  
# What I learned
1. Bugs & fix
- Airflow couldn't find my CSV because the Docker volume path pointed to the wrong folder. I learned that the left side of the volume mapping is a path on my machine and the right side is the path inside the container. Fixing it the left side solved it.
- lots of file path errors

2. Best practices
- storing variable in config.yaml file so the etl_pipeline is reusable for other similar data
- storing credentials in .env files and not in code itself to not release it to public

3. Process lessons
- My first version wasn't reproducible: hardcoded paths, secrets in project folder. I restructured it so a fresh clone runs from the README
