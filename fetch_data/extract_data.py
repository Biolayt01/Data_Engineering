from api_request import fetch_data
import psycopg2
import os
from  dotenv import load_dotenv

load_dotenv()

resp = {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 'location': {'name': 'New York', 'country': 'United States of America', 'region': 'New York', 'lat': '40.714', 'lon': '-74.006', 'timezone_id': 'America/New_York', 'localtime': '2025-11-24 14:28', 'localtime_epoch': 1763994480, 'utc_offset': '-5.0'}, 'current': {'observation_time': '07:28 PM', 'temperature': 11, 'weather_code': 113, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0001_sunny.png'], 'weather_descriptions': ['Sunny'], 'astro': {'sunrise': '06:53 AM', 'sunset': '04:32 PM', 'moonrise': '10:52 AM', 'moonset': '07:56 PM', 'moon_phase': 'Waxing Crescent', 'moon_illumination': 12}, 'air_quality': {'co': '812.85', 'no2': '47.35', 'o3': '11', 'so2': '8.95', 'pm2_5': '28.55', 'pm10': '28.75', 'us-epa-index': '2', 'gb-defra-index': '2'}, 'wind_speed': 18, 'wind_degree': 326, 'wind_dir': 'NW', 'pressure': 1023, 'precip': 0, 'humidity': 36, 'cloudcover': 0, 'feelslike': 9, 'uv_index': 1, 'visibility': 16, 'is_day': 'yes'}}

def connect_to_db():
    try:
        conne = psycopg2.connect(
            host='localhost',
            port=5000,
            dbname= os.getenv('POSTGRES_DB'),
            user= os.getenv('POSTGRES_USER'),
            password= os.getenv('POSTGRES_PASSWORD')
           )
        return conne

    except psycopg2.Error as e:
        print(f'Database connection failed with {e}')
        raise
    

connect_to_db()