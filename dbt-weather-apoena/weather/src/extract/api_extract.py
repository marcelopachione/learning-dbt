import os
import logging
import requests # pyright: ignore[reportMissingModuleSource]
import psycopg2 # pyright: ignore[reportMissingModuleSource]
from dotenv import load_dotenv # pyright: ignore[reportMissingImports]

# Load envs
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## DB Connection
def connecto_to_database():
    logger.info(f"Connecting to the database {os.getenv('POSTGRES_DB')}")

    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=5432,
        )

        logger.info(f"Sucessfully connect to the database {os.getenv('POSTGRES_DB')}")

        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connec to the database {os.getenv('POSTGRES_DB')}, error : {e}")
        return None

## Api Extract
def get_weather_data(city: str):
    api_key = os.getenv('OPENWEATHER_API_KEY')

    if not api_key:
        logging.error("Api key not found")
        return None
    
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'unit': 'metric'
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        weather_data = response.json()

        logger.info(f"Weather data for {city}: {weather_data}.")

        return weather_data
    except requests.RequestException as e:
        logger.error(f"Error featching weather data: {e}.")

        return None

## Create schema and DB tables
def create_schema_and_table(conn):
    
    if not conn:
        logging.error("No database connection available.")
        return None
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS weather;")

            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS weather.city_weather (
                        id SERIAL PRIMARY KEY,
                        city TEXT,
                        temperature FLOAT,
                        weather_description TEXT,
                        wind_speed FLOAT,
                        time TIMESTAMP,
                        inserted_at TIMESTAMP DEFAULT NOW(),
                        timezone TEXT
                    );
                """
            )

            conn.commit()

            logger.info("Schema and table created sucessfully.")
    except psycopg2.Error as e:
        logging.error(f"Error creating schema and table: {e}.")
        return None


def insert_weather_data(conn, city, weather_data):
    
    if conn is None:
        logging.error("No database connection available.")
        return None

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO weather.city_weather (
                    city,
                    temperature,
                    weather_description,
                    wind_speed,
                    time,
                    inserted_at,
                    timezone
                ) VALUES (%s, %s, %s, %s, to_timestamp(%s), NOW(), %s);        
            """, (
                weather_data['name'],
                weather_data['main']['temp'],
                weather_data['weather'][0]['description'],
                weather_data['wind']['speed'],
                weather_data['dt'],
                weather_data['timezone']
            ))
            conn.commit()
            logger.info(f"Weather data for {city} inserted successfully.")

    except psycopg2.Error as e:
        logging.error(f"Error inserting weather data: {e}")
        return None

if __name__ == '__main__':
    city = 'Sao Paulo'
    weather_data = get_weather_data(city)

    conn = connecto_to_database()

    create_schema_and_table(conn)

    insert_weather_data(conn, city, weather_data)

    conn.close()