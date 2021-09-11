#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'
client = boto3.client('dynamodb')

response = client.put_item(
    TableName = TABLE_NAME,
    Item = {
        'id': {'N': '5'},
        'name': {'S': 'Arlyne'},
        'contract_type': {'S': 'permanent'},
        'position': {'S': 'data scientist'},
        'last_accessed': {'N': '20190513'}
    },
    ReturnValues = 'ALL_OLD'
)

print(json.dumps(response, indent=4))
