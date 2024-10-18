import argparse
import signal
import pandas as pd
import os

from create_logger import logger
from stackexchangedump import (
    StackOverflowDump,
    StackOverflowPostParser,
    StackOverflowTagParser,
    StackOverflowUserParser,
)

BATCH_SIZE = 1000

SCHEMA_MAP = {
    "badges": "data/Badges.xml",
    "comments": "data/Comments.xml",
    "posthistory": "data/PostHistory.xml",
    "postlinks": "data/PostLinks.xml",
    "posts": "data/Posts.xml",
    "tags": "data/Tags.xml",
    "users": "data/Users.xml",
    "votes": "data/Votes.xml",
}

PARSER_MAP = {
    "posts": StackOverflowPostParser,
    "users": StackOverflowUserParser,
    "tags": StackOverflowTagParser,
}


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--schema", required=True)
    parser.add_argument("-b", "--batch_size")
    parser.add_argument("-l", "--log-level")
    args = parser.parse_args()

    schema = args.schema
    batch_size = args.batch_size
    log_level = args.log_level

    filename = SCHEMA_MAP[schema]
    schema_parser = PARSER_MAP.get(schema)

    if log_level:
        logger.setLevel(log_level)
    logger.info(f"Dumping {filename} to CSV")

    schema_dump = StackOverflowDump(
        filename=filename,
        root_name=schema,
        batch_size=batch_size or BATCH_SIZE,
        backend="csv",
        sep="\x17",  # custom separator that doesn't exist in the data
        parser=schema_parser,
        progress=True,
    )

    # register handlers for SIGINT and SIGTERM . This is to save our progress when we Ctrl + C or get killed by OS etc
    signal.signal(signal.SIGINT, schema_dump.write_to_progress_file)
    signal.signal(signal.SIGTERM, schema_dump.write_to_progress_file)

    schema_dump.convert_to_csv()

    # read in the final csv, the save as parquet
    csv_file = f"data/{schema}.csv"
    # df = pd.read_csv(csv_file)
