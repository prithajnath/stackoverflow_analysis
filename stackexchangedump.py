import sys
from io import StringIO
from collections import defaultdict
from typing import Optional

import pandas as pd
import pandas_gbq
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod


class StackExchangeParser(ABC):
    @abstractmethod
    def parse(cls, xml) -> dict:
        pass


class StackOverflowPostParser(StackExchangeParser):
    @classmethod
    def parse(cls, xml: str) -> dict:
        root = ET.fromstring(xml)

        result = defaultdict(list)
        for row in root:
            for attribute, attribute_type in [
                ("Id", int),
                ("PostTypeId", int),
                ("AcceptedAnswerId", float),
                ("CreationDate", str),
                ("Score", float),
                ("ViewCount", float),
                ("Body", str),
                ("OwnerUserId", int),
                ("LastEditorUserId", int),
                ("LastEditorDisplayName", str),
                ("LastEditDate", str),
                ("LastActivityDate", str),
                ("Title", str),
                ("Tags", str),
                ("AnswerCount", float),
                ("CommentCount", float),
                ("FavoriteCount", float),
                ("ParentId", int),
                ("OwnerDisplayName", str),
            ]:
                value = row.attrib.get(attribute)
                if value is not None and not isinstance(value, attribute_type):
                    try:
                        attribute_type(value)
                    except:
                        raise ValueError(
                            f"field {attribute} expected {attribute_type} but got {value}"
                        )
                result[attribute].append(value)

        return result


class StackOverflowDump:
    def __init__(
        self,
        filename,
        root_name,
        batch_size,
        backend,
        output=None,
        parser: Optional[StackExchangeParser] = None,
    ):
        self.prefix = "stackoverflow"
        self.parser = parser
        self.filename = filename
        self.output = f"{root_name}.csv" if not output else output
        self.backend = backend
        self.root = root_name
        self.root_open = f"<{root_name}>"
        self.root_close = f"</{root_name}>"
        self.batch_size = batch_size
        self.keep_headers = True
        self.batch_number = 0
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
        def preserve_newlines(value):
            if isinstance(value, str):
                return value.replace("\n", "\\n")
            return value

        xml = self.template.format(xml="".join(self.current_batch))
        if self.parser:
            df = pd.DataFrame(self.parser.parse(xml))
        else:
            df = pd.read_xml(StringIO(xml), converters={"Body": preserve_newlines})
        if self.backend == "csv":
            csv_string = StringIO()
            df.to_csv(csv_string, header=self.keep_headers, index=False, na_rep="")
            with open(self.output, "a") as g:
                g.write(csv_string.getvalue())
        elif self.backend == "bq":
            pandas_gbq.to_gbq(
                df,
                f"stackexchange.{self.prefix}_{self.root}",
                project_id="social-computing-436902",
                if_exists="append",
            )
        if self.batch_number == 0:
            self.keep_headers = False

        self.current_batch = []

    def convert_to_csv(self) -> str:
        with open(self.filename, "r") as f:
            for i, line in enumerate(f):
                # Skip first two lines
                if i > 1 and self.root_close not in line:
                    self.current_batch.append(line.rstrip())
                    if len(self.current_batch) == self.batch_size:
                        self.flush()
                        sys.stdout.write(f"\rFlushed batch {self.batch_number}")
                        self.batch_number += 1

            # Flush everything in buffer
            print(f"flushing last time...")
            self.flush()

        return self.output
