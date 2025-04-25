from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pytz import timezone
import requests
import logging
import pandas as pd
from airflow.models import Variable
from sqlalchemy import create_engine
import os
from pathlib import Path
# from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# print(DEFAULT_DBT_ROOT_PATH,DBT_ROOT_PATH)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",
        profile_args={"schema": "transformed"},
    )
)

# Define the timezone as Nepal Time
nepal_time = timezone('Asia/Kathmandu')

API_KEY = Variable.get("weather_api_key")
API_URL = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q=Kathmandu&dt="
PG_CONN = Variable.get("postgres_conn")
engine = create_engine(PG_CONN)

@task
def extract_weather_data(date: str):
   try:
       response = requests.get(API_URL + date)
       response.raise_for_status()
       data = response.json()
       return pd.json_normalize(
           data["forecast"]["forecastday"][0]["hour"]
       ).assign(
           name=data['location']['name'],
           region=data['location']['region'],
           country=data['location']['country'],
           lat=data['location']['lat'],
           lon=data['location']['lon'],
           tz_id=data['location']['tz_id'],
           localtime_epoch=data['location']['localtime_epoch'],
           localtime=data['location']['localtime']
       )
   except requests.exceptions.RequestException as e:
       logging.error(f"Error fetching weather data: {e}")
       raise
   
@task
def load_to_postgres(df):
   df.to_sql('weather_daily', con=engine, if_exists='append', index=False)
   
@task.branch
def check_table_exists():
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'weather_daily'
        );
    """
    with engine.connect() as connection:
        result = connection.execute(sql).fetchone()
    
    return 'extract_today_flow.start_today' if result[0] else 'extract_backfill_flow.start_backfill'

@task_group(group_id='extract_today_flow')
def extract_today_flow():
    start = EmptyOperator(task_id='start_today')
    date = str(datetime.now().date())
    df = extract_weather_data(date)
    loaded = load_to_postgres(df)
    start >> df >> loaded
    return start

@task_group(group_id='extract_backfill_flow')
def extract_backfill_flow():
    start = EmptyOperator(task_id='start_backfill')
    previous_task = start

    for i in range(1, 8):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')

        extract = extract_weather_data.override(task_id=f'extract_backfill_data_{date}')(date)
        load = load_to_postgres.override(task_id=f'load_backfill_data_{date}')(extract)

        previous_task >> extract >> load
        previous_task = load

    # Optional downstream task, like triggering dbt or summary aggregation
    dbt_task = EmptyOperator(task_id='run_dbt_model')
    previous_task >> dbt_task

    return start

# DAG definition
default_args = {
   'owner': 'airflow',
   'start_date': datetime(2025, 4, 18, 23, 55, tzinfo=nepal_time),
   'retries': 1,
   'retry_delay': timedelta(minutes=1),
}

with DAG(
   'elt_weather',
   default_args=default_args,
   description='Fetch daily weather data from Weather API',
   schedule_interval='0 5 * * *',
   catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_or_skipped')
    branch = check_table_exists()
    
    dbt_task = DbtTaskGroup(
        group_id="dbt_task_group",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH / "weather").as_posix(),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt"),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 5, "trigger_rule": TriggerRule.ONE_SUCCESS},
    )
    
    start >> branch
    branch >> extract_today_flow() >> end
    branch >> extract_backfill_flow() >> end
    end >> dbt_task
    
    