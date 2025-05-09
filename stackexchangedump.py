import csv
import os
import sys
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from io import StringIO
from typing import Optional, Tuple

import pandas as pd
import pandas_gbq

from create_logger import logger


class StackExchangeParser(ABC):
    @abstractmethod
    def parse(cls, xml) -> dict:
        pass


class StackOverflowCRLFParser(StackExchangeParser):

    SCHEMA: list[Tuple] = []
    CRLF_COL = ""

    @classmethod
    def parse(cls, xml: str) -> dict:
        root = ET.fromstring(xml)

        result = defaultdict(list)
        for row in root:
            for attribute, attribute_type in cls.SCHEMA:
                # setting Nones to 0 so pandas reads them as int not floats
                value = row.attrib.get(attribute, "" if attribute_type is str else 0)
                if value is not None and not isinstance(value, attribute_type):
                    try:
                        value = attribute_type(value)
                    except:
                        raise ValueError(
                            f"field {attribute} expected {attribute_type} but got {value}"
                        )
                if attribute == cls.CRLF_COL:
                    # escape crlf
                    value = value.replace("\n", "\\n").replace("\r", "\\r")

                result[attribute].append(value)

        return result


class StackOverflowUserParser(StackOverflowCRLFParser):

    CRLF_COL = "AboutMe"

    SCHEMA = [
        ("Id", int),
        ("Reputation", int),
        ("CreationDate", str),
        ("DisplayName", str),
        ("LastAccessDate", str),
        ("WebsiteUrl", str),
        ("Location", str),
        ("AboutMe", str),
        ("Views", int),
        ("UpVotes", int),
        ("DownVotes", int),
        ("ProfileImageUrl", str),
        ("EmailHash", str),
        ("AccountId", int),
    ]


class StackOverflowPostParser(StackOverflowCRLFParser):

    CRLF_COL = "Body"

    SCHEMA = [
        ("Id", int),
        ("PostTypeId", int),
        ("AcceptedAnswerId", int),
        ("CreationDate", str),
        ("Score", int),
        ("ViewCount", int),
        ("Body", str),
        ("OwnerUserId", int),
        ("OwnerDisplayName", str),
        ("LastEditorUserId", int),
        ("LastEditorDisplayName", str),
        ("LastEditDate", str),
        ("LastActivityDate", str),
        ("Title", str),
        ("Tags", str),
        ("AnswerCount", int),
        ("CommentCount", int),
        ("FavoriteCount", int),
        ("ContentLicense", str),
        ("ParentId", int),
        ("CommunityOwnedDate", str),
        ("ClosedDate", str),
    ]


class StackOverflowTagParser(StackOverflowCRLFParser):
    CRLF_COL = "TagName"
    SCHEMA = [
        ("Id", int),
        ("TagName", str),
        ("Count", int),
        ("ExcerptPostId", int),
        ("WikiPostId", int),
        ("IsModeratorOnly", bool),
        ("IsRequired", bool),
    ]


class StackOverflowDump:
    def __init__(
        self,
        filename,
        root_name,
        batch_size,
        backend,
        sep,
        output=None,
        parser: Optional[StackExchangeParser] = None,
        tempfile_path: Optional[str] = None,
        progress=False,
    ):
        self.prefix = "stackoverflow"
        self.parser = parser
        self.progress = progress
        self.filename = filename
        self.output = f"data/{root_name}.csv" if not output else output
        self.backend = backend
        self.sep = sep
        self.root = root_name
        self.root_open = f"<{root_name}>"
        self.root_close = f"</{root_name}>"
        self.batch_size = batch_size
        self.batch_number = 0
        self.tempfile_path = tempfile_path
        self.current_batch = []
        self.template = (
            f"""<?xml version="1.0" encoding="utf-8"?>
{self.root_open}"""
            + "{xml}"
            + f"""
{self.root_close}
"""
        )

    def flush(self):
        xml = self.template.format(xml="".join(self.current_batch))
        if self.parser:
            df = pd.DataFrame(
                self.parser.parse(xml),
            )
        else:
            df = pd.read_xml(StringIO(xml))
        if self.backend == "csv":
            csv_string = StringIO()
            try:
                df.to_csv(
                    csv_string,
                    header=self.keep_headers,
                    index=False,
                    na_rep="",
                    sep=self.sep,
                )
                with open(self.output, "a", encoding="utf-8") as g:
                    g.write(csv_string.getvalue())
            except Exception as e:
                logger.error(f"Error writing to csv: {e}")
            finally:
                csv_string.close()
        elif self.backend == "bq":
            pandas_gbq.to_gbq(
                df,
                f"stackexchange.{self.prefix}_{self.root}",
                project_id="social-computing-436902",
                if_exists="append",
            )

        self.current_batch = []

    @property
    def keep_headers(self) -> bool:

        if self.tempfile_path:
            if self.batch_number > 0:
                return False
            else:
                return True

        if self.output in os.listdir("."):
            return False
        return True

    def convert_to_csv(self) -> str:
        offset = None
        if self.progress:
            if last_saved := self.get_last_progress():
                _, offset, ts = last_saved

                logger.info(f"Picking up batch {offset} from {ts}")

        with open(self.filename, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                # Skip first two lines
                if i > 1 and self.root_close not in line:

                    self.current_batch.append(line.rstrip())
                    if len(self.current_batch) == self.batch_size:
                        if offset:
                            if offset > self.batch_number:
                                # keep incrementing the batch number
                                self.current_batch = []
                                self.batch_number += 1
                            else:
                                # we are now in the correct batch. Do actual flush
                                self.flush()
                                sys.stdout.write(f"\rFlushed batch {self.batch_number}")
                                self.batch_number += 1
                        else:
                            # don't bother with progress
                            self.flush()
                            sys.stdout.write(f"\rFlushed batch {self.batch_number}")
                            self.batch_number += 1

            # Flush everything in buffer
            logger.info(f"flushing last time...")
            self.flush()

        return self.output

    @property
    def progress_filename(self) -> str:
        return f"data/{self.root}_progress.csv"

    def write_to_progress_file(self, signum, frame):
        fieldnames = ["batch_size", "batch_number", "ts"]
        if self.progress_filename in os.listdir("."):
            headers = False
        else:
            headers = True
        with open(self.progress_filename, "a") as progress_f:
            progress = [
                {
                    "batch_size": self.batch_size,
                    "batch_number": self.batch_number,
                    "ts": datetime.now(),
                }
            ]

            writer = csv.DictWriter(progress_f, fieldnames=fieldnames)
            if headers:
                writer.writeheader()
            writer.writerows(progress)

        sys.exit()

    def get_last_progress(self) -> Optional[Tuple]:
        if self.progress_filename in os.listdir("."):
            progress_df = pd.read_csv(self.progress_filename)
            # find last saved for this batch size
            last_saved = progress_df[progress_df["batch_size"] == self.batch_size]
            if last_saved.shape[0] != 0:
                return tuple(last_saved.iloc[-1])
            else:
                return None
        return None
