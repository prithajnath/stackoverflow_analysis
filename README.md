# StackOverflow data dump analysis

## ETL pipeline

1. Download data as .7z files and extract them
2. Use the `app.py` script to convert the XML file into a CSV
3. Use [dbcrossbar](https://www.dbcrossbar.org/) to dump the CSV into BigQuery

   - dbcrossbar is a Rust CLI tool written by a former co-worker of mine. It's fairly easy to work with but uses CSV as its "interchange format" to move data around, hence the `app.py` script. Once we have a valid CSV, we can use dbrossbar to upsert to a BigQuery table. It should be able to handle data of this scale

   Copying our CSVs to BigQuery should look something like this

   ```
   dbcrossbar cp \
    --if-exists=append \
    csv:posts.csv \
    bigquery:$GCLOUD_PROJECT:my_dataset.posts
   ```

## EDA workflow

We can use the Python BigQuery client to load data from our BigQuery warehouse into a Jupyter notebook. We can leave all the heavy-lifting (things that require incredible compute power for data like this) like sampling, joins and aggregations to BigQuery (via BigQuery SQL) and keep our Python analysis simple and easier to work with.
