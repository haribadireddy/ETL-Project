from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, dag
from pendulum import datetime
import requests
import json

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'

AZURESQL_CONN_ID = 'azuresql_default'
API_CONN_ID = 'open_meteo_api'

@dag(
    start_date=datetime(2025, 4, 25),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Airflow", "retries": 3},
    tags=["weather_etl"],
)

def weatherreport():

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id = API_CONN_ID,method = 'GET')

        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def Transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'lalitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        sql_hook = PostgresHook(azuresql_conn_id = AZURESQL_CONN_ID)
        conn = sql_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            lalitude FLOAT,
            longitute FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        cursor.execute("""
        INSERT INTO weather_data (lalitude, longitude, temperature, windspeed, winddirection,weathercode,timestamp)
        VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            transformed_data['lalitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    weather_data = extract_weather_data()
    transformed_data = Transform_weather_data(weather_data)
    load_weather_data(transformed_data)


weatherreport()