#!/usr/bin/env python
import boto3
import json

TABLE_NAME='elaborate_employee_table'
INDEX_NAME='access_application_index'

client = boto3.client('dynamodb')

response = client.query(
    TableName = TABLE_NAME,
    IndexName = INDEX_NAME,
    KeyConditionExpression='#la = :lav and begins_with(#n, :nv)',
    FilterExpression = '#i < :iv',
    ExpressionAttributeValues = {
        ':lav': {'N':'20190712'},
        ':iv': {'N':'150'},
        ':nv': {'S':'B'}
    },
    ExpressionAttributeNames = {
        '#la': 'last_accessed',
        '#n': 'name',
        '#i': 'id'
    },
    ProjectionExpression = '#n'
)

print(json.dumps(response['Items'], indent=4))
