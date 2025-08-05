
import os
from dotenv import load_dotenv
import requests

load_dotenv()
api_key = os.getenv("WEATHERSTACK_API_ACCESS_KEY")
city = "Tokyo"

api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city}"

def fetch_data():
    print("Fetching data from API...")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print("API request successful.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        raise


# fetch_data()

def mock_fetch_data():
    print("Mocking API data fetch...")
    return {'request': {'type': 'City', 'query': 'Tokyo, Japan', 'language': 'en', 'unit': 'm'}, 'location': {'name': 'Tokyo', 'country': 'Japan', 'region': 'Tokyo', 'lat': '35.690', 'lon': '139.692', 'timezone_id': 'Asia/Tokyo', 'localtime': '2025-08-03 17:07', 'localtime_epoch': 1754240820, 'utc_offset': '9.0'}, 'current': {'observation_time': '08:07 AM', 'temperature': 35, 'weather_code': 116, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0002_sunny_intervals.png'], 'weather_descriptions': ['Partly cloudy'], 'astro': {'sunrise': '04:51 AM', 'sunset': '06:44 PM', 'moonrise': '01:56 PM', 'moonset': '11:34 PM', 'moon_phase': 'Waxing Gibbous', 'moon_illumination': 64}, 'air_quality': {'co': '469.9', 'no2': '80.475', 'o3': '61', 'so2': '61.605', 'pm2_5': '63.455', 'pm10': '64.565', 'us-epa-index': '3', 'gb-defra-index': '3'}, 'wind_speed': 21, 'wind_degree': 139, 'wind_dir': 'SE', 'pressure': 1001, 'precip': 0, 'humidity': 56, 'cloudcover': 50, 'feelslike': 42, 'uv_index': 1, 'visibility': 10, 'is_day': 'yes'}}

