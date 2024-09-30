import sys
from io import StringIO

import pandas as pd


class StackOverflowDump:
    def __init__(self, filename, root_name, batch_size):
        self.filename = filename
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
        xml = self.template.format(xml="".join(self.current_batch))
        df = pd.read_xml(StringIO(xml))
        csv_string = StringIO()
        # print(f"Using header={keep_headers}")
        df.to_csv(csv_string, header=self.keep_headers)
        if self.batch_number == 0:
            self.keep_headers = False

        with open(f"{self.root}.csv", "a") as g:
            g.write(csv_string.getvalue())
        self.current_batch = []

    def convert_to_csv(self):
        with open(self.filename, "r") as f:
            for i, line in enumerate(f):
                # Skip first two lines
                if i > 1 and self.root_close not in line:
                    self.current_batch.append(line)
                    if len(self.current_batch) == self.batch_size:
                        self.flush()
                        sys.stdout.write(f"\rFlushed batch {self.batch_number}")
                        self.batch_number += 1

            # Flush everything in buffer
            print(f"flushing last time...")
            self.flush()
