from stackexchangedump import StackOverflowDump

BATCH_SIZE = 6


if __name__ == "__main__":
    post_dump = StackOverflowDump(
        filename="Posts.xml", root_name="posts", batch_size=BATCH_SIZE, backend="csv"
    )

    post_dump.convert_to_csv()
