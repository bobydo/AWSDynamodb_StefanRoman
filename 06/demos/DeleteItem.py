#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'

client = boto3.client('dynamodb')

response = client.delete_item(
    TableName = TABLE_NAME,
    Key = {'id': {'N': '5'}},
    ReturnValues = 'ALL_OLD'
)

print(json.dumps(response['Attributes'], indent=4))
