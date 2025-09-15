import os
import logging
import requests
import psycopg2
from dotenv import load_dotenv

# Load envs
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

        logger.info(f"Weather data for {city}: {weather_data}")

        return weather_data
    except requests.RequestException as e:
        logger.error(f"Error featching weather data: {e}")

        return None

if __name__ == '__main__':
    city = 'Sao Paulo'
    get_weather_data(city)