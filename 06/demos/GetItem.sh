#!/bin/bash

TABLE_NAME='elaborate_employee_table'

aws \
dynamodb \
get-item \
--table-name ${TABLE_NAME} \
--key '{"id": {"N": "156"}}' | jq .
