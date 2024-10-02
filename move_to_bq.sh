#!/usr/bin/env bash

   dbcrossbar cp \
    --if-exists=upsert-on:Id \
    --temporary=gs://stackexchange_bucket/stackoverflow \
    --schema=postgres-sql:create_posts.sql \
    csv:posts.csv \
    bigquery:social-computing-436902:stackexchange.stackoverflow_posts