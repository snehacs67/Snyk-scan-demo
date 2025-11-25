
import requests
import boto3
import json
from datetime import datetime
import pytz
import uuid

# AWS region and DynamoDB configuration
region_name = 'us-east-1'
table_name = 'Bitcoin_price_store'

# API endpoint for Bitcoin price
api_url = 'https://api.coinbase.com/v2/prices/btc-usd/spot'

# Get current time in IST
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).isoformat()

# Create DynamoDB client
dynamodb = boto3.client('dynamodb', region_name=region_name)

# Function to insert item into DynamoDB
def put_item_to_dynamodb(item):
    dynamodb.put_item(TableName=table_name, Item=item)

def main():
    # Fetch data from REST API
    response = requests.get(api_url)
    data = response.json()

    # Prepare data for DynamoDB
    data_to_ingest = {
        "amount": {"S": data["data"]["amount"]},
        "base": {"S": data["data"]["base"]},
        "currency": {"S": data["data"]["currency"]},
        "timestamp": {"S": current_time},
        "uuid": {"S": str(uuid.uuid4())}
    }

    # Insert into DynamoDB
    put_item_to_dynamodb(data_to_ingest)
    print(f"Item {data_to_ingest} added to DynamoDB table {table_name}.")
    print("Data transfer complete.")

if __name__ == "__main__":
    main()
