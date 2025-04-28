from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from airflow.models import Variable

PG_CONN = Variable.get("postgres_conn")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='el_csv_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['csv', 'postgres', 'etl'],
) as dag:

    @task
    def extract():
        input_path = '/opt/airflow/dags/weather_data.csv'
        df = pd.read_csv(input_path)
        return df

    @task
    def load_to_postgres(df):

        engine = create_engine(PG_CONN)

        # Load to PostgreSQL
        df.to_sql('weather', engine, if_exists='replace', index=False)

    # Task flow
    extract_task=extract()
    load_task=load_to_postgres(extract_task)

    extract_task >> load_task
