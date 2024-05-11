from google.cloud import storage
import os
import requests
import pandas as pd
from datetime import datetime

# Fetching environment variables
access_key = os.environ.get('ACCESS_KEY')
airport = os.environ.get('AIRPORT')

def departutre_flight_information(access_key, airport):
    """Fetch the real-time flights information from aviationstack API."""
    url = f'http://api.aviationstack.com/v1/flights?access_key={access_key}&dep_iata={airport}'
    response = requests.get(url)
    data = response.json()

    flights_info = []
    for flight in data["data"]:
        flight_dict = {
                "flight_date": flight["flight_date"],
                "flight_status": flight["flight_status"],
                "departure_airport": flight["departure"]["airport"],
                "departure_timezone": flight["departure"]["timezone"],
                "departure_iata": flight["departure"]["iata"],
                "departure_delay": flight["departure"]["delay"],
                "departure_scheduled": flight["departure"]["scheduled"],
                "departure_estimated": flight["departure"]["estimated"],
                "departure_actual": flight["departure"]["actual"],
                "departure_estimated_runway": flight["departure"]["estimated_runway"],
                "departure_actual_runway": flight["departure"]["actual_runway"],
                "arrival_airport": flight["arrival"]["airport"],
                "arrival_timezone": flight["arrival"]["timezone"],
                "arrival_iata": flight["arrival"]["iata"],
                "arrival_delay": flight["arrival"]["delay"],
                "arrival_scheduled": flight["arrival"]["scheduled"],
                "arrival_estimated": flight["arrival"]["estimated"],
                "arrival_actual": flight["arrival"]["actual"],
                "arrival_estimated_runway": flight["arrival"]["estimated_runway"],
                "arrival_actual_runway": flight["arrival"]["actual_runway"],
                "airline_name": flight["airline"]["name"],
                "airline_iata": flight["airline"]["iata"],
                "flight_number": flight["flight"]["number"],
                "flight_iata": flight["flight"]["iata"],
                # Check if "live" data exists before accessing its keys
                "live_update_time": flight["live"]["updated"] if flight.get("live") else None,
                "live_latitude": flight["live"]["latitude"] if flight.get("live") else None,
                "live_longitude": flight["live"]["longitude"] if flight.get("live") else None,
                "live_altitude": flight["live"]["altitude"] if flight.get("live") else None,
                "live_speed_horizontal": flight["live"]["speed_horizontal"] if flight.get("live") else None,
                "live_speed_vertical": flight["live"]["speed_vertical"] if flight.get("live") else None,
                "live_is_ground": flight["live"]["is_ground"] if flight.get("live") else None
            }
        flights_info.append(flight_dict)

    return flights_info


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def main():
    departure_flight_list = departutre_flight_information(access_key, airport)
    df = pd.DataFrame(departure_flight_list)
    
    # Generate a unique filename to avoid overwrites
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'real_time_flights_{timestamp}.csv'
    df.to_csv(filename, index=False)
    
    # Upload the file to GCS
    bucket_name = 'test_3hours_flight_data'  # Replace with your actual bucket name
    upload_to_gcs(bucket_name, filename, f'flight_data/{filename}')

if __name__ == '__main__':
    main()






