#!/bin/bash

TABLE_NAME='elaborate_employee_table'
INDEX_NAME='scan_index'

aws \
dynamodb \
scan \
--return-consumed-capacity INDEXES \
--table-name ${TABLE_NAME} \
--index-name ${INDEX_NAME} \
--debug > /dev/null | jq .
