import boto3
import csv
import io

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = 'vetri-devops-bucket'
FILE_NAME = 'inventory.csv'
TABLE_NAME = 'InventoryTable'

REQUIRED_FIELDS = ['Hostname', 'IP Address', 'Environment', 'Role', 'OS', 'Status', 'Owner', 'Location']

def lambda_handler(event, context):
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(content))

        table = dynamodb.Table(TABLE_NAME)

        count = 0
        with table.batch_writer() as batch:
            for row in csv_reader:
                # Skip empty rows or rows missing required fields
                if not row or not all(field in row and row[field].strip() for field in REQUIRED_FIELDS):
                    continue

                # Clean up whitespace
                cleaned_row = {k.strip(): v.strip() for k, v in row.items()}

                # Insert only if all fields are non-empty
                if all(cleaned_row.get(field) for field in REQUIRED_FIELDS):
                    batch.put_item(Item={
                        'Hostname': cleaned_row['Hostname'],
                        'IPAddress': cleaned_row['IP Address'],
                        'Environment': cleaned_row['Environment'],
                        'Role': cleaned_row['Role'],
                        'OS': cleaned_row['OS'],
                        'Status': cleaned_row['Status'],
                        'Owner': cleaned_row['Owner'],
                        'Location': cleaned_row['Location']
                    })
                    count += 1

        return {
            'statusCode': 200,
            'body': f'InventoryTable updated successfully. Total valid items inserted: {count}'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
