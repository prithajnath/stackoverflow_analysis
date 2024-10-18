from lxml import etree
import csv
import argparse
from tqdm import tqdm
import os


def getting_headers(xml_file):
    headers = set()
    tag = "row"

    file_size = os.path.getsize(xml_file)

    with open(xml_file, "rb") as f:
        with tqdm(
            total=file_size, unit="B", unit_scale=True, desc="Processing XML"
        ) as pbar:

            class TqdmFile(object):
                def __init__(self, file, tqdm_instance):
                    self.file = file
                    self.tqdm = tqdm_instance

                def read(self, size):
                    data = self.file.read(size)
                    self.tqdm.update(len(data))
                    return data

                def __getattr__(self, attr):
                    return getattr(self.file, attr)

            tqdm_file = TqdmFile(f, pbar)
            context = etree.iterparse(tqdm_file, events=("end",), tag=tag)

            for event, elem in context:
                headers.update(elem.attrib.keys())

                # removing to clear memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

    base_name = os.path.splitext(os.path.basename(xml_file))[0]
    header_file_name = f"data/{base_name}_headers.csv"

    with open(header_file_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(sorted(headers))

    print(f"Headers successfully written to {header_file_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract CSV headers from a large Stack Overflow XML data dump."
    )
    parser.add_argument(
        "-i",
        "--xml_file",
        required=True,
        help="Path to the XML file (e.g., Posts.xml).",
    )
    args = parser.parse_args()
    getting_headers(args.xml_file)
