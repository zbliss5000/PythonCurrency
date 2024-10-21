import json
import boto3
import csv
import io
from decimal import Decimal

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    
    processed_bucket = 'processed_properties'
    table_name = 'PropertiesTable'

    # Retrieving the S3 object based on event triggers
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_filename = event['Records'][0]['s3']['object']['key']
    obj = s3.get_object(Bucket=raw_bucket, Key=csv_filename)
    csv_data = obj['Body'].read().decode('utf-8').splitlines()
    
    processed_rows = []
    required_keys = ['zpid', 'streetAddress', 'unit', 'bedrooms', 
                     'bathrooms', 'homeType', 'priceChange', 'zipcode', 'city', 
                     'state', 'country', 'livingArea', 'taxAssessedValue', 
                     'priceReduction', 'datePriceChanged', 'homeStatus', 'price', 'currency']
    
    # Parsing CSV data
    reader = csv.DictReader(csv_data)
    for row in reader:
        filtered_row = {key: row[key] for key in required_keys if key in row}
        
        # Perform currency conversion
        if 'price' in filtered_row and 'currency' in filtered_row:
            filtered_row['price'] = convert_currency(Decimal(filtered_row['price']), filtered_row['currency'])
            filtered_row['currency'] = 'USD'  # Normalize to USD after conversion
        
        processed_rows.append(filtered_row)
    
    # Upload processed data to DynamoDB
    if processed_rows:
        upload_to_dynamodb(table_name, processed_rows)
        move_file_to_processed_bucket(raw_bucket, processed_bucket, csv_filename)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Currency normalization and processing complete!')
    }

# Function to convert currencies (CAD, MXN to USD)
def convert_currency(price, currency):
    conversion_rates = {
        'CAD': Decimal('0.75'),  # CAD to USD conversion rate
        'MXN': Decimal('0.05')   # MXN to USD conversion rate
    }
    
    if currency in conversion_rates:
        return price * conversion_rates[currency]
    return price  # No conversion needed for USD or unknown currencies

# Function to upload data to DynamoDB using batch writer
def upload_to_dynamodb(table_name, items):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

# Function to move files from raw bucket to processed bucket
def move_file_to_processed_bucket(raw_bucket_name, processed_bucket_name, file_key):
    s3 = boto3.resource('s3')
    
    # Copy the file to the processed bucket
    copy_source = {
        'Bucket': raw_bucket_name,
        'Key': file_key
    }
    s3.meta.client.copy(copy_source, processed_bucket_name, file_key)
    
    # Delete the original file from the raw bucket
    s3.Object(raw_bucket_name, file_key).delete()

