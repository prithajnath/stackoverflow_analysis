import argparse
import os


def check_pipe_in_xml(file_path, chunk_size=1024 * 1024, char="\x17"):
    """
    Check if the char is present is XML
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            position = 0
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                index = chunk.find(char)
                if index != -1:
                    position += index
                    return True, position
                position += len(chunk)
        return False, -1
    except FileNotFoundError:
        print(f"Error: File '{file_path}' does not exist.")
        return False, -1
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        return False, -1


def main():
    parser = argparse.ArgumentParser(
        description=f"Check if the char is present in an XML file."
    )
    parser.add_argument(
        "xml_file", type=str, help="Path to the XML file to be checked."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1024 * 1024,
        help="Size of each chunk to read in bytes (default: 1MB).",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.xml_file):
        print(f"Error: The file '{args.xml_file}' does not exist.")
        return

    found, pos = check_pipe_in_xml(args.xml_file, args.chunk_size)
    if found:
        print(f"Char found in '{args.xml_file}' at byte position {pos}.")
    else:
        print(f"No char found in '{args.xml_file}'.")


if __name__ == "__main__":
    main()