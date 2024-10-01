#! /usr/bin/bash

   dbcrossbar cp \
    --if-exists=overwrite \
    --temporary=gs://stackexchange_bucket/stackoverflow \
    --schema=postgres-sql:create_posts.sql \
    csv:posts.csv \
    bigquery:social-computing-436902:stackexchange.posts