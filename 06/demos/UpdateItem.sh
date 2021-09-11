#!/bin/bash

TABLE_NAME='elaborate_employee_table'

# aws \
# dynamodb \
# update-item \
# --table-name ${TABLE_NAME} \
# --key file://update_item_key.json \
# --expression-attribute-values file://update_item_values.json \
# --expression-attribute-names file://update_item_expression_names.json \
# --update-expression "SET #ads = :a, #kds = :k" | jq .


aws \
dynamodb \
get-item \
--table-name ${TABLE_NAME} \
--key file://update_item_key.json \
--projection-expression "addresses[0].street" | jq .
