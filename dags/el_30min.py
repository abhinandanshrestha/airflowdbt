# key='7bf13464e9e04c8984f134342232606'
# https://www.weatherapi.com/api-explorer.aspx

from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz
import requests
from datetime import datetime, timedelta
import logging
from airflow.models import Variable
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

# Define the API URL
API_KEY = Variable.get("weather_api_key") # Replace with your actual API key
API_URL = f'http://api.weatherapi.com/v1/current.json?key={API_KEY}&q=Kathmandu&aqi=yes'

@task
def create_tables():
    # Set up the PostgreSQL hook
    conn_id = 'postgres'  # Set your connection ID here
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    # Get the connection object
    conn = postgres_hook.get_conn()

    # Create a cursor
    cursor = conn.cursor()

    # Define the table name
    table_name = 'weather_data'

    # Create the table if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            location_name VARCHAR(255),
            country VARCHAR(255),
            lat FLOAT,
            lon FLOAT,
            tz_id VARCHAR(255),
            localtime_epoch INT,
            "localtime" TIMESTAMP,
            temperature_c FLOAT,
            temperature_f FLOAT,
            condition_text VARCHAR(255),
            condition_code INT,
            wind_mph FLOAT,
            wind_kph FLOAT,
            wind_degree INT,
            wind_dir VARCHAR(50),
            pressure_mb FLOAT,
            precip_mm FLOAT,
            humidity INT,
            cloud INT,
            feelslike_c FLOAT,
            feelslike_f FLOAT,
            windchill_c FLOAT,
            windchill_f FLOAT,
            heatindex_c FLOAT,
            heatindex_f FLOAT,
            dewpoint_c FLOAT,
            dewpoint_f FLOAT,
            vis_km FLOAT,
            vis_miles FLOAT,
            uv FLOAT,
            gust_mph FLOAT,
            gust_kph FLOAT,
            last_updated_epoch INT,
            last_updated TIMESTAMP,
            co FLOAT,
            no2 FLOAT,
            o3 FLOAT,
            so2 FLOAT,
            pm2_5 FLOAT,
            pm10 FLOAT,
            us_epa_index INT,
            gb_defra_index INT
        )
    """)
    conn.commit()

# Function to fetch weather data from the API
@task
def extract_weather_data():
    try:
        # Make a request to the weather API
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # If the response is successful, parse the JSON data
        data = response.json()
        logging.info("Kathmandu Weather data fetched successfully")
        logging.info("Response data: %s", data)
        
        # Flatten the data structure into a single dictionary for DataFrame
        weather_data = {
            'location_name': data['location']['name'],
            'country': data['location']['country'],
            'lat': data['location']['lat'],
            'lon': data['location']['lon'],
            'tz_id': data['location']['tz_id'],
            'localtime_epoch': data['location']['localtime_epoch'],
            'localtime': datetime.strptime(data['location']['localtime'], '%Y-%m-%d %H:%M'),
            'temperature_c': data['current']['temp_c'],
            'temperature_f': data['current']['temp_f'],
            'condition_text': data['current']['condition']['text'],
            'condition_code': data['current']['condition']['code'],
            'wind_mph': data['current']['wind_mph'],
            'wind_kph': data['current']['wind_kph'],
            'wind_degree': data['current']['wind_degree'],
            'wind_dir': data['current']['wind_dir'],
            'pressure_mb': data['current']['pressure_mb'],
            'precip_mm': data['current']['precip_mm'],
            'humidity': data['current']['humidity'],
            'cloud': data['current']['cloud'],
            'feelslike_c': data['current']['feelslike_c'],
            'feelslike_f': data['current']['feelslike_f'],
            'windchill_c': data['current']['windchill_c'],
            'windchill_f': data['current']['windchill_f'],
            'heatindex_c': data['current']['heatindex_c'],
            'heatindex_f': data['current']['heatindex_f'],
            'dewpoint_c': data['current']['dewpoint_c'],
            'dewpoint_f': data['current']['dewpoint_f'],
            'vis_km': data['current']['vis_km'],
            'vis_miles': data['current']['vis_miles'],
            'uv': data['current']['uv'],
            'gust_mph': data['current']['gust_mph'],
            'gust_kph': data['current']['gust_kph'],
            'last_updated_epoch': data['current']['last_updated_epoch'],
            'last_updated': datetime.strptime(data['current']['last_updated'], '%Y-%m-%d %H:%M'),
            # Adding air quality data
            'co': data['current']['air_quality']['co'],
            'no2': data['current']['air_quality']['no2'],
            'o3': data['current']['air_quality']['o3'],
            'so2': data['current']['air_quality']['so2'],
            'pm2_5': data['current']['air_quality']['pm2_5'],
            'pm10': data['current']['air_quality']['pm10'],
            'us_epa_index': data['current']['air_quality']['us-epa-index'],
            'gb_defra_index': data['current']['air_quality']['gb-defra-index']
        }

        # Create a pandas dataframe from the flattened dictionary
        df = pd.DataFrame([weather_data])
        return df
        # # For example, log the current temperature
        # current_temp = data['current']['temp_c']
        # logging.info(f"Current temperature in Kathmandu: {current_temp}°C")
        
        # # You can add more processing here, such as saving the data to a file or database
        # # For example, saving to a local file
        # with open('/path/to/your/weather_data.json', 'w') as f:
        #     f.write(response.text)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        raise

@task
def load_to_postgres(df):
    # Set up the PostgreSQL hook
    conn_id = 'postgres'  # Set your connection ID here
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    # Get the connection object
    conn = postgres_hook.get_conn()

    # Create a cursor
    cursor = conn.cursor()

    # Define the table name
    table_name = 'weather_data'

    # Prepare the INSERT INTO SQL statement
    insert_query = f"""
        INSERT INTO {table_name} (
            location_name, country, lat, lon, tz_id, localtime_epoch, "localtime", 
            temperature_c, temperature_f, condition_text, condition_code, wind_mph, wind_kph, 
            wind_degree, wind_dir, pressure_mb, precip_mm, humidity, cloud, 
            feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, 
            dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, 
            last_updated_epoch, last_updated, co, no2, o3, so2, pm2_5, pm10, 
            us_epa_index, gb_defra_index
        ) VALUES (
            %(location_name)s, %(country)s, %(lat)s, %(lon)s, %(tz_id)s, %(localtime_epoch)s, %(localtime)s, 
            %(temperature_c)s, %(temperature_f)s, %(condition_text)s, %(condition_code)s, %(wind_mph)s, %(wind_kph)s, 
            %(wind_degree)s, %(wind_dir)s, %(pressure_mb)s, %(precip_mm)s, %(humidity)s, %(cloud)s, 
            %(feelslike_c)s, %(feelslike_f)s, %(windchill_c)s, %(windchill_f)s, %(heatindex_c)s, %(heatindex_f)s, 
            %(dewpoint_c)s, %(dewpoint_f)s, %(vis_km)s, %(vis_miles)s, %(uv)s, %(gust_mph)s, %(gust_kph)s, 
            %(last_updated_epoch)s, %(last_updated)s, %(co)s, %(no2)s, %(o3)s, %(so2)s, %(pm2_5)s, %(pm10)s, 
            %(us_epa_index)s, %(gb_defra_index)s
        )
    """

    # Convert DataFrame to a list of dictionaries
    records = df.to_dict(orient='records')

    # Execute the insert query for each record
    try:
        for record in records:
            cursor.execute(insert_query, record)
        
        # Commit the transaction
        conn.commit()
        logging.info("Weather data with air quality loaded successfully into PostgreSQL")

    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        conn.rollback()  # Rollback in case of error

    # Close the cursor and connection
    cursor.close()
    conn.close()


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG using 'with DAG()'
with DAG(
    'fetch_weather_data',
    default_args=default_args,
    description='A DAG to fetch weather data from the Weather API',
    # schedule_interval='* * * * *', # Every minute
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,  # Do not backfill if the DAG runs after the start_date
) as dag:

    # Define the tasks
    create_tables_task = create_tables()
    extract_weather_df = extract_weather_data()
    load_df_to_postgres=load_to_postgres(extract_weather_df)
    
    create_tables_task >> extract_weather_df >> load_df_to_postgres
    
    
