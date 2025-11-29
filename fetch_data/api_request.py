import requests as req
import os
from  dotenv import load_dotenv

##load environment variable
load_dotenv()

api_key = os.getenv('api_key')


api_url = f"https://api.weatherstack.com/current?access_key={api_key}&query=New York"


def fetch_data() ->None:
    '''
    This function fetches the data from the WeatherStack API website for New York city. 
    It returns current and weather data which will be stored on Postgresql database.
    '''
    try:
        print('Fetching weather data from the WeatherStack API website')

        response = req.get(api_url)
        response.raise_for_status()
        print('Api response received successfully')
        return response.json()
    
    except req.exceptions.RequestException as e:
        print(f'An error occured {e}')
        raise


if __name__=='__main__':
    fetch_data()


