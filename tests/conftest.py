import pytest
import os


from collections import namedtuple
from tempfile import NamedTemporaryFile
from stackexchangedump import StackOverflowDump


@pytest.fixture(scope="session")
def stackoverflow_posts():
    filename = os.path.join("tests", "Posts.xml")
    root = "posts"

    StackOverflow = namedtuple("StackOverflow", ["filename", "root"])
    return StackOverflow(filename=filename, root=root)


@pytest.fixture(scope="session")
def stackoverflow_csv_dumper(stackoverflow_posts):
    with NamedTemporaryFile("a") as f:
        so_dumper = StackOverflowDump(
            filename=stackoverflow_posts.filename,
            root_name=stackoverflow_posts.root,
            batch_size=6,
            backend="csv",
            output=f"{f.name}.csv",
        )

        yield so_dumper
