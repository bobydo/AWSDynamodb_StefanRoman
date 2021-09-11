#!/bin/bash

TABLE_NAME='elaborate_employee_table'
INDEX_NAME='position_application_index'

aws \
dynamodb \
query \
--table-name ${TABLE_NAME} \
--key-condition-expression "#i = :n" \
--expression-attribute-values file://query_attr_values.json \
--expression-attribute-names file://query_attr_names.json \
--profile test | jq .
