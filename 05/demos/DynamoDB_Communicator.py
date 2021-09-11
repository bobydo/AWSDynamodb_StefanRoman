import boto3
import names
import random
import datetime


# return dynamodb client to authenticate
def dynamodb():
    client = boto3.client('dynamodb', region_name=TABLE_REGION)
    return client


# the function will produce a random date depending on set range
def random_date():
    day = random.randint(10, 20)
    month = random.randint(5, 7)
    year = 2019
    date = datetime.date(year, month, day).strftime("%Y%m%d")
    return date


# the function willl generate a random job position
def random_position():
    positions = [
        'HR', 'devops', 'data scientist', 'developer', 'janitor',
        'office coordinator'
    ]
    return random.choice(positions)


# the function will generate random first name
def random_contract_type():
    contract_types = ['permanent', 'intern', 'contract']
    return random.choice(contract_types)


# the function writes to the table, item with ID partition key
def write_items(id_number):
    client = dynamodb()
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "id": {
                "S": id_number
            },
            "contract_type": {
                "S": random_contract_type()
            },
            "last_accessed": {
                "N": random_date()
            },
            "name": {
                "S": names.get_first_name()
            },
            "position": {
                "S": random_position()
            }
        })
    print('writing item number {}'.format(id_number))


# the function reads from the table, item with ID partition key
def read_items(id_number):
    client = dynamodb()
    client.get_item(TableName=TABLE_NAME, Key={"id": {"S": id_number}})
    print('reading item number {}'.format(id_number))


# set the number of requests you'd like to make to the table
REQUESTS = 10000
# set table name, where you want to sent the requests
TABLE_NAME = 'elaborate_employee_table'
# in what region does the table reside
TABLE_REGION = 'us-west-2'
# which action you'd like to do write / read
ACTION = 'read'


# for loop that will go through request number and either write or read from the table
# depending on action type
for request in range(REQUESTS):
    if ACTION == 'write':
        write_items(str(request))
    elif ACTION == 'read':
        read_items(str(request))
    else:
        pass
