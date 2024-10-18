import os
from collections import namedtuple
from tempfile import NamedTemporaryFile

import pytest

from stackexchangedump import StackOverflowDump, StackOverflowPostParser


@pytest.fixture(scope="session")
def stackoverflow_posts():
    filename = os.path.join("tests", "test_Posts.xml")
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
            parser=StackOverflowPostParser,
            tempfile_path=f.name,
        )

        yield so_dumper


@pytest.fixture(scope="session")
def stackoverflow_csv_dumper_with_progress_and_parser(stackoverflow_posts):
    with NamedTemporaryFile("a") as f:
        so_dumper = StackOverflowDump(
            filename=stackoverflow_posts.filename,
            root_name=stackoverflow_posts.root,
            batch_size=1,
            backend="csv",
            output=f"{f.name}.csv",
            parser=StackOverflowPostParser,
            progress=True,
            tempfile_path=f.name,
        )

        yield so_dumper
