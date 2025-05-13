import boto3
import csv
import io

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = 'vetri-devops-bucket'
FILE_NAME = 'inventory.csv'
TABLE_NAME = 'InventoryTable'

def lambda_handler(event, context):
    try:
        # Get the CSV file from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        content = response['Body'].read().decode('utf-8')

        # Read the CSV content as dictionary
        csv_reader = csv.DictReader(io.StringIO(content), delimiter='\t')

        table = dynamodb.Table(TABLE_NAME)

        with table.batch_writer() as batch:
            for row in csv_reader:
                # Skip completely empty or malformed rows
                if not row or not row.get('Hostname') or not row.get('IP Address'):
                    continue

                # Strip whitespace from keys and values
                cleaned_row = {k.strip(): v.strip() for k, v in row.items() if v}

                # Put the cleaned item into DynamoDB
                batch.put_item(Item={
                    'Hostname': cleaned_row.get('Hostname', ''),
                    'IPAddress': cleaned_row.get('IP Address', ''),
                    'Environment': cleaned_row.get('Environment', ''),
                    'Role': cleaned_row.get('Role', ''),
                    'OS': cleaned_row.get('OS', ''),
                    'Status': cleaned_row.get('Status', ''),
                    'Owner': cleaned_row.get('Owner', ''),
                    'Location': cleaned_row.get('Location', '')
                })

        return {
            'statusCode': 200,
            'body': 'InventoryTable updated successfully from CSV file.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
