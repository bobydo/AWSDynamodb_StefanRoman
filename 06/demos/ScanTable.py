#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'
INDEX_NAME='scan_index'

client = boto3.client('dynamodb')

response = client.scan(
    TableName = TABLE_NAME,
    IndexName = INDEX_NAME,
    ReturnConsumedCapacity = 'INDEXES'
)

print(json.dumps(response['ConsumedCapacity'], indent=4))
