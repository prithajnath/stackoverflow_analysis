import argparse
import signal

from create_logger import logger
from stackexchangedump import StackOverflowDump, StackOverflowPostParser

BATCH_SIZE = 1000

SCHEMA_MAP = {
    "posts": "Posts.xml",
    "users": "Users.xml",
    "votes": "Votes.xml",
    "tags": "Tags.xml",
    "badges": "Badges.xml",
}

PARSER_MAP = {"posts": StackOverflowPostParser}


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

    logger.setLevel(log_level)
    logger.info(f"Dumping {filename} to CSV")

    schema_dump = StackOverflowDump(
        filename=filename,
        root_name=schema,
        batch_size=batch_size or BATCH_SIZE,
        backend="csv",
        parser=schema_parser,
        progress=True,
    )

    # register handlers for SIGINT, SIGKILL etc. This is to save our progress when we Ctrl + C or get killed by OS etc
    signal.signal(signal.SIGINT, schema_dump.write_to_progress_file)

    schema_dump.convert_to_csv()
