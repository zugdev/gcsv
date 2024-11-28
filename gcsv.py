import csv
import zlib
import io
from typing import List, Iterator
import pandas as pd

class GCSVReader:
    """Read a compressed GCSV file as if it were a CSV file."""
    def __init__(self, gcsv_file: str):
        self.gcsv_file = gcsv_file
        self.decompressed_stream = io.StringIO(self._decompress_gcsv())

    def __enter__(self):
        """Open the GCSV file and prepare for reading."""
        self.decompressed_stream = io.StringIO(self._decompress_gcsv())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the stream on exit."""
        self.close()

    def _decompress_gcsv(self) -> str:
        """Decompress the entire GCSV file into a string."""
        decompressed_data = []
        with open(self.gcsv_file, 'rb') as f:
            while True:
                size_data = f.read(4)
                if not size_data:
                    break
                chunk_size = int.from_bytes(size_data, 'big')
                compressed_chunk = f.read(chunk_size)
                decompressed_data.append(zlib.decompress(compressed_chunk).decode('utf-8'))
        return ''.join(decompressed_data)

    def read(self) -> List[List[str]]:
        """Read all rows from the GCSV file."""
        reader = csv.reader(self.decompressed_stream)
        return list(reader)

    def __iter__(self) -> Iterator[List[str]]:
        """Make the reader an iterable object."""
        self.decompressed_stream.seek(0)
        reader = csv.reader(self.decompressed_stream)
        return iter(reader)

    def close(self):
        """Close the underlying stream."""
        self.decompressed_stream.close()

class GCSVWritter:
    """Write data to a GCSV file with compression."""
    def __init__(self, gcsv_file: str):
        self.gcsv_file = gcsv_file
        self.buffer = io.StringIO()

    def __enter__(self):
        """Prepare the writer."""
        self.buffer = io.StringIO()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Flush and close on exit."""
        self.close()

    def write_row(self, row: List[str]):
        """Write a single row to the internal buffer."""
        writer = csv.writer(self.buffer)
        writer.writerow(row)

    def write_rows(self, rows: List[List[str]]):
        """Write multiple rows to the internal buffer."""
        writer = csv.writer(self.buffer)
        writer.writerows(rows)

    def _compress_and_write(self):
        """Compress the buffered data and write to the GCSV file."""
        compressed_data = zlib.compress(self.buffer.getvalue().encode('utf-8'))
        with open(self.gcsv_file, 'ab') as f:
            f.write(len(compressed_data).to_bytes(4, 'big'))
            f.write(compressed_data)
        self.buffer = io.StringIO()  # Reset the buffer

    def close(self):
        """Flush the buffer and close the writer."""
        self._compress_and_write()
        self.buffer.close()


def read_gcsv(gcsv_file: str, **kwargs) -> "pd.DataFrame":
    """
    Read a GCSV file into a pandas DataFrame.
    :param gcsv_file: Path to the input GCSV file.
    :param kwargs: Additional arguments passed to pd.DataFrame.
    :return: pandas DataFrame.
    """
    with GCSVReader(gcsv_file) as reader:
        data = list(reader)  # Read all rows
    return pd.DataFrame(data[1:], columns=data[0], **kwargs)  # uses the first row as column headers


def to_gcsv(df: "pd.DataFrame", gcsv_file: str, **kwargs):
    """
    Write a pandas DataFrame to a GCSV file.
    :param df: pandas DataFrame to write.
    :param gcsv_file: Path to the output GCSV file.
    :param kwargs: Additional arguments passed to DataFrame.to_csv.
    """
    with GCSVWritter(gcsv_file) as writer:
        csv_string = df.to_csv(index=False, **kwargs)
        rows = csv_string.splitlines()
        writer.write_rows([row.split(",") for row in rows])