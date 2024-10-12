#!/usr/bin/env bash

# Usage: ./move_to_bq.sh badges

dbcrossbar cp \
    --if-exists=upsert-on:Id \
    --temporary=gs://stackexchange_bucket/stackoverflow \
    --schema=postgres-sql:create_$1.sql \
    csv:$1.csv \
    bigquery:social-computing-436902:stackexchange.stackoverflow_$1