from google.cloud import storage
import os
import requests
import pandas as pd
from datetime import datetime

# Fetching environment variables
api_key = os.environ.get('API_KEY')
lat = os.environ.get('LAT')
lon = os.environ.get('LON')

def get_weather_forecast(api_key, lat, lon):
    """Fetch the 5 Day Weather Forecast from OpenWeatherMap API."""
    url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)
    data = response.json()

    forecasts = []
    for forecast in data['list']:
        forecast_dict = {
            'dt_txt': forecast['dt_txt'],
            'main_temp': forecast['main']['temp'],
            'feels_like': forecast['main']['feels_like'],
            'temp_min': forecast['main']['temp_min'],
            'temp_max': forecast['main']['temp_max'],
            'pressure': forecast['main']['pressure'],
            'sea_level': forecast['main']['sea_level'],
            'grnd_level': forecast['main']['grnd_level'],
            'humidity': forecast['main']['humidity'],
            'temp_kf': forecast['main']['temp_kf'],
            'weather_main': forecast['weather'][0]['main'],
            'weather_description': forecast['weather'][0]['description'],
            'weather_icon': forecast['weather'][0]['icon'],
            'clouds': forecast['clouds']['all'],
            'wind_speed': forecast['wind']['speed'],
            'wind_deg': forecast['wind']['deg'],
            'wind_gust': forecast.get('wind', {}).get('gust', 0),
            'visibility': forecast['visibility'],
            'pop': forecast['pop'],
            'rain_3h': forecast.get('rain', {}).get('3h', 0),
            'sys_pod': forecast['sys']['pod']
        }
        forecasts.append(forecast_dict)

    return forecasts

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def main():
    # Generate a unique filename to avoid overwrites
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'5_days_3_hour_forecast_{timestamp}.csv'
    
    # Call the API and convert the result to a DataFrame
    forecast_list = get_weather_forecast(api_key, lat, lon)
    df = pd.DataFrame(forecast_list)
    
    # Save the DataFrame to a CSV file
    df.to_csv(filename, index=False)
    
    # Upload the file to GCS
    bucket_name = 'test_5day_3hour_weather_forecast'  # Replace with your actual bucket name
    upload_to_gcs(bucket_name, filename, f'weather_forecasts/{filename}')

if __name__ == '__main__':
    main()
