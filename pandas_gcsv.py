import pandas as pd
import os
import tempfile
from compress import gcsv_compress
from decompress import gcsv_decompress

def read_gcsv(gcsv_file: str, **kwargs) -> pd.DataFrame:
    """
    Read a GCSV file into a pandas DataFrame.
    :param gcsv_file: Path to the input GCSV file.
    :param kwargs: Additional arguments passed to pd.DataFrame.
    :return: pandas DataFrame.
    """
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_csv_path = temp_file.name

    try:
        gcsv_decompress(gcsv_file, temp_csv_path)
        return pd.read_csv(temp_csv_path, **kwargs)
    finally:
        os.remove(temp_csv_path)

def to_gcsv(df: pd.DataFrame, gcsv_file: str, chunk_size=10, max_threads=16, **kwargs):
    """
    Write a pandas DataFrame to a GCSV file.
    :param df: pandas DataFrame to write.
    :param gcsv_file: Path to the output GCSV file.
    :param chunk_size: Chunk size in MB for compression.
    :param max_threads: Number of threads for compression.
    :param kwargs: Additional arguments passed to DataFrame.to_csv.
    """
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_csv_path = temp_file.name
        df.to_csv(temp_csv_path, index=False, **kwargs)

    try:
        gcsv_compress(temp_csv_path, gcsv_file, chunk_size=chunk_size, max_threads=max_threads)
    finally:
        os.remove(temp_csv_path)