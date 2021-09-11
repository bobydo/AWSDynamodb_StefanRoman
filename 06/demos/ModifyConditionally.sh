#!/bin/bash

TABLE_NAME='elaborate_employee_table'

# aws \
# dynamodb \
# put-item \
# --table-name ${TABLE_NAME} \
# --item file://modify_conditionally_item_overwrite.json \
# --condition-expression "attribute_not_exists(id)" \
# --return-values ALL_OLD | jq .

aws \
dynamodb \
update-item \
--table-name ${TABLE_NAME} \
--key '{"id":{"N":"10"}}' \
--expression-attribute-values file://modify_conditionally_values.json \
--expression-attribute-names file://modify_conditionally_names.json \
--update-expression "SET #ec = :ecn" \
--condition-expression "attribute_not_exists(#ec) and attribute_exists(#ad.#st) and #ct = :ctv" \
--return-values UPDATED_NEW | jq .
