#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'
client = boto3.client('dynamodb')

response = client.batch_write_item(
    RequestItems = {
    TABLE_NAME: [
      {
        'PutRequest': {
          'Item': {
            'id': {'N':'111'},
            'status': {'S': 'Python batch write 1'}
          }
        }
      },
      {
        'PutRequest': {
          'Item': {
            'id': {'N':'112'},
            'status': {'S': 'Python batch write 2'}
          }
        }
      }
    ]
  }
)

print(json.dumps(response, sort_keys=True, indent=4))
