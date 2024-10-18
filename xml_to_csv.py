import os
import csv
import xml.etree.ElementTree as ET
import argparse
from tqdm import tqdm
import time


def parse_xml_to_csv(xml_file, header_file, chunk_size=10000):
    start = time.time()
    base_name = os.path.basename(xml_file).replace(".xml", "")
    csv_file = os.path.join(os.path.dirname(xml_file), f"{base_name}.csv")

    # Getting all headers
    with open(header_file, "r") as hf:
        headers = hf.read().strip().split(",")

    print(f"{time.time() - start:.2f} | Base name created and headers read")

    # Write to CSV with custom delimiter \x17
    with open(csv_file, "w", newline="", encoding="utf-8") as csvf:
        print(f"{time.time() - start:.2f} | Writing to CSV")
        writer = csv.DictWriter(csvf, fieldnames=headers, delimiter="\x17")
        writer.writeheader()

        print(f"{time.time() - start:.2f} | Parsing XML")
        context = ET.iterparse(xml_file, events=("start", "end"))
        context = iter(context)
        event, root = next(context)

        total_size = os.path.getsize(xml_file)

        rows = []
        processed_size = 0
        with tqdm(
            total=total_size, desc="Processing XML", unit="B", unit_scale=True
        ) as pbar:
            for event, elem in context:
                if event == "end" and elem.tag == "row":
                    row_data = {col: elem.attrib.get(col, "") for col in headers}
                    rows.append(row_data)

                    if len(rows) >= chunk_size:
                        writer.writerows(rows)
                        rows = []  # Clear rows list to save memory
                        root.clear()  # Clear the root to free memory

                    # Process the element size and update the progress bar
                    elem_str = ET.tostring(elem, encoding="utf-8")
                    processed_size += len(elem_str)
                    pbar.update(len(elem_str))
                    elem.clear()  # Clearing element to save memory

            if rows:  # Writing any final rows
                writer.writerows(rows)
                pbar.update(len(rows))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert large XML to CSV in chunks.")
    parser.add_argument("-i", "--xml_file", help="Path to the XML file")

    args = parser.parse_args()

    xml_file = args.xml_file
    header_file = (
        f"headers/{os.path.basename(xml_file).replace('.xml', '')}_headers.csv"
    )

    parse_xml_to_csv(xml_file, header_file)
