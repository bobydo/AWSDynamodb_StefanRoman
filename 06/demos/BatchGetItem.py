#!/usr/bin/env python
import boto3
import json

TABLE_NAME_1='elaborate_employee_table'
TABLE_NAME_2='simple_employee_table'

client = boto3.client('dynamodb')

response = client.batch_get_item(
    RequestItems = {
    TABLE_NAME_1: {
      'Keys': [
        {'id': {'N': '111'}},
        {'id': {'N': '112'}}
      ]
    }
  }
)

print(json.dumps(response['Responses'], indent=4))
