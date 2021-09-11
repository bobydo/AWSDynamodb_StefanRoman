#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'

client = boto3.client('dynamodb')

response = client.get_item(
    TableName = TABLE_NAME,
    Key = {'id': {'N': '5'}})

print(json.dumps(response['Item'], indent=4))
