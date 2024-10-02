import signal

from stackexchangedump import StackOverflowDump, StackOverflowPostParser

BATCH_SIZE = 1000


if __name__ == "__main__":
    post_dump = StackOverflowDump(
        filename="Posts.xml",
        root_name="posts",
        batch_size=BATCH_SIZE,
        backend="csv",
        parser=StackOverflowPostParser,
        progress=True,
    )

    # register handlers for SIGINT, SIGKILL etc. This is to save our progress when we Ctrl + C or get killed by OS etc
    signal.signal(signal.SIGINT, post_dump.write_to_progress_file)

    post_dump.convert_to_csv()
