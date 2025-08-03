
import os
from dotenv import load_dotenv
import requests

load_dotenv()
api_key = os.getenv("WEATHERSTACK_API_ACCESS_KEY")
city = "Tokyo"

api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city}"

def fetch_data():
    response = requests.get(api_url)
    print(response)