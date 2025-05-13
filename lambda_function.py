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
        response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(content))

        table = dynamodb.Table(TABLE_NAME)

        for row in csv_reader:
            table.put_item(Item={
                'Hostname': row['Hostname'],
                'IPAddress': row['IP Address'],
                'Environment': row['Environment'],
                'Role': row['Role'],
                'OS': row['OS'],
                'Status': row['Status'],
                'Owner': row['Owner'],
                'Location': row['Location']
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
