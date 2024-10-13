import argparse
import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.parquet as pq


def csv_to_parquet_pyarrow(csv_file_path, parquet_file_path, chunk_size=10**6):
    # memory_map doesn't read entire file into memory
    with pa.memory_map(csv_file_path, "r") as source:
        csv_reader = pc.open_csv(
            source,
            read_options=pc.ReadOptions(block_size=chunk_size * 100),
            parse_options=pc.ParseOptions(delimiter=","),
            convert_options=pc.ConvertOptions(strings_can_be_null=True),
        )

        parquet_writer = None

        for record_batch in csv_reader:
            # If writer is not initialized,
            # create it with the schema from the first batch
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    parquet_file_path, record_batch.schema
                )

            parquet_writer.write_batch(record_batch)

        if parquet_writer:
            parquet_writer.close()

    print(f"Successfully converted {csv_file_path} to {parquet_file_path}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Convert large CSV to Parquet using PyArrow."
    )
    parser.add_argument("-i", "--csv-input", help="Path to the input CSV file.")
    parser.add_argument(
        "-o", "--parquet-output", help="Path to the output Parquet file."
    )
    parser.add_argument(
        "-s", "--chunk-size", type=int, default=10**6, help="Number of rows per chunk."
    )

    args = parser.parse_args()
    csv_to_parquet_pyarrow(args.csv_input, args.parquet_output, args.chunk_size)
