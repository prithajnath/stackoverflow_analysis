# StackOverflow data dump analysis

## ETL pipeline

1. Download data as .7z files and extract them
2. Use the `app.py` script to convert the XML file into a CSV
3. Use [dbcrossbar](https://www.dbcrossbar.org/) to dump the CSV into BigQuery (or some other warehouse like RedShift etc). Most warehouses have good connectors for Python

   - dbcrossbar is a Rust command line tool written by a former co-worker of mine. The interface is kinda wonky but it works great for large volumnes of data. It uses CSV as its "interchange format" to move data around, hence the `app.py` script. Once we have a valid CSV, we can use dbrossbar to upsert to a BigQuery table. It should be able to handle data of this scale

   So a basic pipeline looks like this now

   ```
   $ python app.py
   WARNING: writing without headers
   Picking up batch 2077 from 2024-10-01 21:46:10.993452
   Flushed batch 2552^C(stackoverflow_analysis)
   $ ./move_to_bq.sh
   ```

   The `move_to_bq.sh` uses `dbcrossbar` to chunk big `post.csv` CSV file into smaller files and streams them to Google Cloud storage bucket, and then copies them into a single BigQuery table. The `app.py` script can be interrupted with a Ctrl + C if you wish to stop and save progress. When you run the script again it should pick up from where it left off. The script uses a `*_progress.csv` file to keep track of interrupts. Make sure your `posts.csv` and `posts_progress.csv` files are in sync if you wish to start from where you left off. Otherwise you can start from scratch by removing both files

   The `move_to_bq.sh` script will UPSERT all the data so you can run it as many times as you want without worrying about creating duplicate rows

## EDA workflow

We can use the Python BigQuery client to load data from our BigQuery warehouse into a Jupyter notebook. We can leave all the heavy-lifting jobs like sampling, joins and aggregations to BigQuery (via BigQuery SQL) and keep our Python analysis simple and easier to work with.

## Running tests

Run the test suite with the following command

```
python -m pytest -vs
```
