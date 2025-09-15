import json
import boto3
from decimal import Decimal

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WeatherHistory') # Your table name

def lambda_handler(event, context):
    try:
        # Scan the table to get all items
        response = table.scan()
        items = response.get('Items', [])

        # Sort items by timestamp in descending order (newest first)
        items.sort(key=lambda x: x['timestamp'], reverse=True)

        return {
            'statusCode': 200,
            'headers': {
                # This header is REQUIRED for your browser to be able to call the API
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(items, cls=DecimalEncoder)
        }
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps('Error reading from DynamoDB')}