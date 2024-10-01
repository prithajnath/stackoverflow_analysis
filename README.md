# StackOverflow data dump analysis

## ETL pipeline

1. Download data as .7z files and extract them
2. Use the `app.py` script to convert the XML file into a CSV
3. Use [dbcrossbar](https://www.dbcrossbar.org/) to dump the CSV into BigQuery (or some other warehouse like RedShift etc). Most warehouses have good connectors for Python

   - dbcrossbar is a Rust command line tool written by a former co-worker of mine. The interface is kinda wonky but it works great for large volumnes of data. It uses CSV as its "interchange format" to move data around, hence the `app.py` script. Once we have a valid CSV, we can use dbrossbar to upsert to a BigQuery table. It should be able to handle data of this scale

   Copying our CSVs to BigQuery should look something like this

   ```
   dbcrossbar cp \
    --if-exists=overwrite \
    --temporary=gs://stackexchange_bucket/stackoverflow \
    --schema=postgres-sql:create_posts.sql \
    csv:posts.csv \
    bigquery:social-computing-436902:stackexchange.posts
   ```

   If this doesn't work, we can look at other options (Spark?)

## EDA workflow

We can use the Python BigQuery client to load data from our BigQuery warehouse into a Jupyter notebook. We can leave all the heavy-lifting jobs like sampling, joins and aggregations to BigQuery (via BigQuery SQL) and keep our Python analysis simple and easier to work with.

## Running tests

Run the test suite with the following command

```
python -m pytest -vs
```
