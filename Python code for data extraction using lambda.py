import json
import os
from datetime import datetime
import requests
import boto3

# Initialize S3 client
s3_client = boto3.client('s3')

# Get WeatherAPI key from environment variable
weather_api_key = os.environ['WEATHER_API_KEY']

def get_weather_data(city):
    api_url = "http://api.weatherapi.com/v1/current.json"
    params = {
        "q": city,
        "key": weather_api_key
    }
    response = requests.get(api_url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        # Log the error or raise an exception
        print(f"Error fetching data for {city}. Status code: {response.status_code}")
        return None

def lambda_handler(event, context):
    # S3 bucket configuration
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    
    cities = ["Bangalore", "Delhi", "Mumbai", "Chennai", "Kashmir", "Dehradun", "Kochi", "Kerela", "Hyderabad", "Sikkim"]
    
    for city in cities:
        data = get_weather_data(city)

        if data:
            # Generate filename based on city, current date, hour, and minute
            current_timestamp = datetime.utcnow()
            filename = f"{city}_{current_timestamp.strftime('%Y%m%d%H%M')}_raw.json"
            
            # Save the raw data in S3
            s3_raw_key = f"raw-data/{city}/{filename}"
            s3_client.put_object(Body=json.dumps(data), Bucket=s3_bucket_name, Key=s3_raw_key)
            
            print(f"Raw data stored in S3 for {city} - Filename: {filename}")

    return {
        'statusCode': 200
    }
