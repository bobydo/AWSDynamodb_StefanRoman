#!/bin/bash

TABLE_NAME='elaborate_employee_table'

aws \
dynamodb \
put-item \
--table-name ${TABLE_NAME} \
--item file://put_itemput item.json \
--return-values ALL_OLD