#!/bin/bash

TABLE_NAME='elaborate_employee_table'

aws dynamodb update-item \
--table-name ${TABLE_NAME} \
--key '{"id":{"N":"20"}}' \
--update-expression "SET #ca = #ca + :val" \
--expression-attribute-values '{":val": {"N":"5"}}' \
--expression-attribute-names '{"#ca": "count_accessed"}' \
--return-values UPDATED_NEW | jq .
