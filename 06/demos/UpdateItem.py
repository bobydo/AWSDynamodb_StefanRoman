#!/usr/bin/env python
import boto3
import json
import random

TABLE_NAME = 'elaborate_employee_table'

id_numbers = [10, 20, 30, 40, 50, 60, 70, 90, 100]

client = boto3.client('dynamodb')
for id_number in id_numbers:
    response = client.update_item(
        TableName=TABLE_NAME,
        Key={'id': {'N': '{}'.format(id_number)}},
        UpdateExpression='SET #ct = :ctv',
        ExpressionAttributeValues={':ctv': {'S':'permanent'}},
        ExpressionAttributeNames={'#ct': 'contract_type'},
        ReturnValues='UPDATED_OLD')

    print(json.dumps(response['Attributes'], indent=4))
