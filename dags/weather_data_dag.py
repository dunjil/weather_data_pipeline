import requests
from airflow import DAG
from airflow.decorators import dag, task
from pendulum import datetime
import pendulum
from datetime import timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from airflow.models import Variable

api_key = Variable.get('api_key')

# '02cf4841a84e25170a1d7e274c91a907'
cities = [
    "Tokyo",
    "New York",
    "London",
    "Paris",
    "Sydney",
    "Cairo"
]

default_args = {
    'owner': 'Duna Jilang',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="open_weather_dag",
    description="Fetches Data from Open Weather API and loads into Postgres Database on Azure Portal",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    default_args=default_args,
    schedule_interval="*/2 * * * *",
    catchup=False,
)
def weather_api():
    
    @task()
    def extract():
        """Authenticates to Open Weather API to fect data for each city with the provided API key """
        weather_raw_data = [] 

        for city in cities:
            url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                weather_raw_data.append(data)
            else:
                logging.error(f"Failed to retrieve data for {city}: {response.status_code} - {response.text}")
        return weather_raw_data

    @task()
    def transform(raw_data: list[dict]) -> list[dict]:
        """This function definition transforms and enriches the data and returns a clean list of dictionaries"""
        cleaned_data = []
        
        try:
            for data in raw_data:
                city_name = data.get('name')
                country = data.get('sys', {}).get('country')
                lat = data.get('coord', {}).get('lat')
                lon = data.get('coord', {}).get('lon')
                timezone_offset = data.get('timezone')
                weather_description = data.get('weather', [{}])[0].get('description')
                avg_temp = data.get('main', {}).get('temp')  # Already in Celsius
                pressure = data.get('main', {}).get('pressure')
                humidity = data.get('main', {}).get('humidity')
                clouds = data.get('clouds', {}).get('all')
                rain = data.get('rain', {}).get('1h', 0)

                
                dt = data['dt']
                current_time_gmt = pendulum.from_timestamp(dt + timezone_offset, tz='UTC').to_datetime_string()

                cleaned_data.append({
                    'city_name': city_name,
                    'country': country,
                    'lat': lat,
                    'lon': lon,
                    'time_zone': timezone_offset,
                    'time': current_time_gmt,
                    'avg_temp': avg_temp,
                    'main_weather_description': weather_description,
                    'clouds': clouds,
                    'rain': rain,
                    'pressure': pressure,
                    'humidity': humidity
                })

                logging.info(f"Cleaned data: {cleaned_data}")

        except Exception as e:
            logging.error(f"An error occurred during transformation: {e}")
            raise

        return cleaned_data
        
    @task()
    def load(cleaned_data: list[dict]):
        """Load the cleaned data into PostgreSQL. It uses the connection ID loaded from the connections in admin"""
        try:
            hook = PostgresHook(postgres_conn_id='postgres_conn_id')
            connection = hook.get_conn()
            cursor = connection.cursor()

            for record in cleaned_data:
                cursor.execute("""
                    INSERT INTO weather_data (
                        city_name, country, lat, lon, time_zone, time, avg_temp, 
                        main_weather_description, clouds, rain, pressure, humidity
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    record['city_name'],
                    record['country'],
                    record['lat'],
                    record['lon'],
                    record['time_zone'],
                    record['time'],
                    record['avg_temp'],
                    record['main_weather_description'],
                    record['clouds'],
                    record['rain'],
                    record['pressure'],
                    record['humidity']
                ))

            
            connection.commit()

        except Exception as e:
            logging.error(f"Error loading data into PostgreSQL: {e}")
            raise

        finally:
            
            cursor.close()
            connection.close()

    
    raw_data = extract()
    cleaned_data = transform(raw_data)
    load(cleaned_data)

weather_api_dag = weather_api()
