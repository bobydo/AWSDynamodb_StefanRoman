#!/bin/bash

aws \
dynamodb \
batch-get-item \
--request-items file://batch_get_items.json | jq .
