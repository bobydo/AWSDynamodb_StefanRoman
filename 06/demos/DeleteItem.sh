#!/bin/bash

TABLE_NAME='elaborate_employee_table'

aws \
dynamodb \
delete-item \
--table-name ${TABLE_NAME} \
--key '{"id": {"N": "157"}}' \
--return-values ALL_OLD
