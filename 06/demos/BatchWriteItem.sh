#!/bin/bash

aws \
dynamodb \
batch-write-item \
--request-items file://batch_write_items.json | jq .
