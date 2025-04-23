# # postgresql://airflow:airflow@192.168.88.40:9878/postgres
# https://www.weatherapi.com/api-explorer.aspx#history
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from pytz import timezone
import requests
from datetime import datetime, timedelta
import logging
from airflow.models import Variable
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from sqlalchemy import create_engine

# Define the timezone as Nepal Time
nepal_time = timezone('Asia/Kathmandu')

API_KEY = Variable.get("weather_api_key")
API_URL = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q=Kathmandu&dt="
PG_CONN = Variable.get("postgres_conn")
engine = create_engine(PG_CONN)

@task
def extract_weather_data(date):
    try:
        response = requests.get(API_URL + date)
        response.raise_for_status()
        data = response.json()
        hourly_data = []

        for hour in data["forecast"]["forecastday"][0]["hour"]:
            hourly_data.append({
                "name": data['location']['name'],
                "region": data['location']['region'],
                "country": data['location']['country'],
                "lat": data['location']['lat'],
                "lon": data['location']['lon'],
                "tz_id": data['location']['tz_id'],
                "localtime_epoch": data['location']['localtime_epoch'],
                "localtime": data['location']['localtime'],
                "time": hour["time"],
                "temp_c": hour["temp_c"],
                "temp_f": hour["temp_f"],
                "condition_text": hour["condition"]["text"],
                "condition_icon": hour["condition"]["icon"],
                "wind_mph": hour["wind_mph"],
                "wind_kph": hour["wind_kph"],
                "wind_degree": hour["wind_degree"],
                "wind_dir": hour["wind_dir"],
                "pressure_mb": hour["pressure_mb"],
                "pressure_in": hour["pressure_in"],
                "precip_mm": hour["precip_mm"],
                "precip_in": hour["precip_in"],
                "snow_cm": hour["snow_cm"],
                "humidity": hour["humidity"],
                "cloud": hour["cloud"],
                "feelslike_c": hour["feelslike_c"],
                "feelslike_f": hour["feelslike_f"],
                "windchill_c": hour["windchill_c"],
                "windchill_f": hour["windchill_f"],
                "heatindex_c": hour["heatindex_c"],
                "heatindex_f": hour["heatindex_f"],
                "dewpoint_c": hour["dewpoint_c"],
                "dewpoint_f": hour["dewpoint_f"],
                "will_it_rain": hour["will_it_rain"],
                "chance_of_rain": hour["chance_of_rain"],
                "will_it_snow": hour["will_it_snow"],
                "chance_of_snow": hour["chance_of_snow"],
                "vis_km": hour["vis_km"],
                "vis_miles": hour["vis_miles"],
                "gust_mph": hour["gust_mph"],
                "gust_kph": hour["gust_kph"],
                "uv": hour["uv"]
            })
        return pd.DataFrame(hourly_data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        raise

@task
def load_to_postgres(df):
    df.to_sql('weather_daily', con=engine, if_exists='append', index=False)

def branch_table_exists():
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables 
            WHERE table_name = 'weather_daily'
        );
    """
    with engine.connect() as connection:
        result = connection.execute(sql).fetchone()
        
    if result[0]:
        return 'extract_today_branch'  # ✅ an actual task_id
    else:
        return 'extract_backfill_branch'  # ✅ first backfill task

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 18, 23, 55, tzinfo=nepal_time),  # Set start time at 23:55 Nepal Time
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_weather_data_daily',
    default_args=default_args,
    description='A DAG to fetch weather data from the Weather API',
    # schedule_interval=None,
    # schedule_interval='@daily',
    # schedule_interval='15 23 * * *',
    schedule_interval='0 5 * * *',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    branching = BranchPythonOperator(
        task_id='check_if_table_exists',
        python_callable=branch_table_exists,
    )

    extract_today = EmptyOperator(task_id='extract_today_branch')
    extract_backfill = EmptyOperator(task_id='extract_backfill_branch')
    
    # Today's data extraction and loading
    # The .override() method allows you to do this without having to create a new operator from scratch, providing flexibility when dynamically generating tasks.
    extract_today_data = extract_weather_data.override(task_id='extract_today_data')(str(datetime.now().date()))
    load_today = load_to_postgres.override(task_id='load_today')(extract_today_data)

    extract_today >> extract_today_data >> load_today >> end

    # Sequential backfill tasks
    previous_task = extract_backfill

    for i in range(1, 8):
        date = str((datetime.now() - timedelta(days=i)).date())
        extract = extract_weather_data.override(task_id=f'extract_backfill_data_{date}')(date)
        load = load_to_postgres.override(task_id=f'load_backfill_data_{date}')(extract)

        previous_task >> extract >> load
        previous_task = load

    previous_task >> end

    # Set main flow
    start >> branching
    branching >> extract_today
    branching >> extract_backfill
    
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.empty import EmptyOperator
# import pytz
# import requests
# from datetime import datetime, timedelta
# import logging
# from airflow.models import Variable
# import pandas as pd
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.decorators import task
# from sqlalchemy import create_engine

# # Define the API URL
# API_KEY = Variable.get("weather_api_key") # Replace with your actual API key
# API_URL = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q=Kathmandu&dt="

# # PostgreSQL connection string (replace with your actual credentials)
# # Example: 'postgresql://username:password@localhost:5432/mydatabase'
# PG_CONN = Variable.get("postgres_conn") # Replace with your actual PostgreSQL connection string
# engine = create_engine(PG_CONN)

# @task
# def check_if_table_exists():
#     sql = """
#         SELECT EXISTS (
#             SELECT 1
#             FROM information_schema.tables 
#             WHERE table_name = 'weather_daily'
#         );
#     """
#     # Execute the query and fetch the result
#     with engine.connect() as connection:
#         result = connection.execute(sql).fetchone()
#     if result[0]:
#         return True
#     else:
#         return False

# # Function to fetch weather data from the API
# @task
# def extract_weather_data(date):
#     try:
#         # Make a request to the weather API
#         response = requests.get(API_URL+date)
#         response.raise_for_status()  # Raise an exception for HTTP errors

#         # If the response is successful, parse the JSON data
#         data = response.json()
#         logging.info("Kathmandu Weather data fetched successfully")
#         logging.info("Response data: %s", data)
        
#         # Extracting the hourly data
#         hourly_data = []

#         for hour in data["forecast"]["forecastday"][0]["hour"]:
#             hourly_data.append({
#                 "name" : data['location']['name'],
#                 "region" : data['location']['region'],
#                 "country" : data['location']['country'],
#                 "lat" : data['location']['lat'],
#                 "lon" : data['location']['lon'],
#                 "tz_id" : data['location']['tz_id'],
#                 "localtime_epoch" : data['location']['localtime_epoch'],
#                 "localtime" : data['location']['localtime'],
#                 "time": hour["time"],
#                 "temp_c": hour["temp_c"],
#                 "temp_f": hour["temp_f"],
#                 "condition_text": hour["condition"]["text"],
#                 "condition_icon": hour["condition"]["icon"],
#                 "wind_mph": hour["wind_mph"],
#                 "wind_kph": hour["wind_kph"],
#                 "wind_degree": hour["wind_degree"],
#                 "wind_dir": hour["wind_dir"],
#                 "pressure_mb": hour["pressure_mb"],
#                 "pressure_in": hour["pressure_in"],
#                 "precip_mm": hour["precip_mm"],
#                 "precip_in": hour["precip_in"],
#                 "snow_cm": hour["snow_cm"],
#                 "humidity": hour["humidity"],
#                 "cloud": hour["cloud"],
#                 "feelslike_c": hour["feelslike_c"],
#                 "feelslike_f": hour["feelslike_f"],
#                 "windchill_c": hour["windchill_c"],
#                 "windchill_f": hour["windchill_f"],
#                 "heatindex_c": hour["heatindex_c"],
#                 "heatindex_f": hour["heatindex_f"],
#                 "dewpoint_c": hour["dewpoint_c"],
#                 "dewpoint_f": hour["dewpoint_f"],
#                 "will_it_rain": hour["will_it_rain"],
#                 "chance_of_rain": hour["chance_of_rain"],
#                 "will_it_snow": hour["will_it_snow"],
#                 "chance_of_snow": hour["chance_of_snow"],
#                 "vis_km": hour["vis_km"],
#                 "vis_miles": hour["vis_miles"],
#                 "gust_mph": hour["gust_mph"],
#                 "gust_kph": hour["gust_kph"],
#                 "uv": hour["uv"]
#             })

#         # Create a DataFrame
#         df = pd.DataFrame(hourly_data)
#         return df

#     except requests.exceptions.RequestException as e:
#         logging.error(f"Error fetching weather data: {e}")
#         raise
    
# @task
# def load_to_postgres(df):
#     # Append DataFrame to the table (replace 'my_table' with your table name)
#     df.to_sql('weather_daily', con=engine, if_exists='append', index=False)
    
    
# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 4, 23),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Define the DAG using 'with DAG()'
# with DAG(
#     'fetch_weather_data_daily',
#     default_args=default_args,
#     description='A DAG to fetch weather data from the Weather API',
#     # schedule_interval='* * * * *', # Every minute
#     schedule_interval=None,  # Every 30 minutes
#     catchup=False,  # Do not backfill if the DAG runs after the start_date
# ) as dag:

#     start = EmptyOperator(task_id='start')
    
#     check_task=check_if_table_exists()
#     if check_task:
#         extract_weather_df = extract_weather_data(str(datetime.now().date()))
#         load_df_to_postgres=load_to_postgres(extract_weather_df)
        
#         extract_weather_df >> load_df_to_postgres
#     else:
#         for date in [str(datetime.today() - timedelta(days=i)) for i in range(7)]:
#             extract_weather_df = extract_weather_data(date)
#             load_df_to_postgres=load_to_postgres(extract_weather_df)
            
#             extract_weather_df >> load_df_to_postgres


