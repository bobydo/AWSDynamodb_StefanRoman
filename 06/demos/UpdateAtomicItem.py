#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'
client = boto3.client('dynamodb')

response = client.update_item(
    TableName=TABLE_NAME,
    Key={'id': {'N': '30'}},
    UpdateExpression='SET #ca = #ca + :val',
    ExpressionAttributeValues={':val': {'N': '120'}},
    ExpressionAttributeNames={'#ca': 'count_accessed'},
    ReturnValues='UPDATED_NEW')

print(json.dumps(response['Attributes'], indent=4))
