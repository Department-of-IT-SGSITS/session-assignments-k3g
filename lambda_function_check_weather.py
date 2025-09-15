import os
import json
import boto3
import requests
from datetime import datetime # Import the datetime library

# Initialize the clients
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb') # NEW: Initialize DynamoDB resource

def lambda_handler(event, context):
    # --- Get Environment Variables ---
    API_KEY = os.environ['WEATHER_API_KEY']
    CITY_NAME = os.environ['CITY_NAME']
    SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
    TEMP_THRESHOLD = int(os.environ['TEMP_THRESHOLD'])
    TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME'] # NEW: Get table name

    # --- Set up the DynamoDB table object ---
    table = dynamodb.Table(TABLE_NAME) # NEW: Get the table object

    print(f"Fetching weather for {CITY_NAME}...")
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={API_KEY}&units=metric"

    try:
        response = requests.get(url)
        response.raise_for_status()
        weather_data = response.json()

        current_temp = weather_data['main']['temp']
        description = weather_data['weather'][0]['description']
        print(f"Successfully fetched weather. Current temperature is {current_temp}°C.")

        # --- NEW: Save the data to DynamoDB ---
        # Create a unique timestamp for the record
        timestamp = datetime.utcnow().isoformat()

        # Put the item into the table
        table.put_item(
            Item={
                'timestamp': timestamp,
                'city': CITY_NAME,
                'temperature': str(current_temp), # DynamoDB prefers strings for numbers
                'description': description
            }
        )
        print("Successfully saved weather data to DynamoDB.")
        # --- End of new section ---

        # --- Check Temperature Threshold ---
        if current_temp > TEMP_THRESHOLD:
            print(f"Temperature is above the threshold. Sending alert.")
            message = f"Weather Alert for {CITY_NAME}!\n\nThe current temperature is {current_temp}°C, which is above your threshold of {TEMP_THRESHOLD}°C.\n\nFull details: {description}."
            subject = f"High Temperature Alert for {CITY_NAME}"

            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject=subject
            )
            print("Alert sent successfully.")
        else:
            print(f"Temperature is not above the threshold. No alert sent.")

    except Exception as e:
        print(f"An error occurred: {e}")
        return {'statusCode': 500, 'body': json.dumps(f'Error: {e}')}

    return {'statusCode': 200, 'body': json.dumps('Function executed successfully!')}