import pandas as pd
import io
import zlib
from concurrent.futures import ThreadPoolExecutor

def read_gcsv(gcsv_file: str, **kwargs) -> pd.DataFrame:
    """
    Read a GCSV file into a pandas DataFrame.
    :param gcsv_file: Path to the input GCSV file.
    :param kwargs: Additional arguments passed to pd.DataFrame.
    :return: pandas DataFrame.
    """
    decompressed_data = _decompress_gcsv_to_memory(gcsv_file)

    # Use io.StringIO to simulate a file-like object from the decompressed string
    return pd.read_csv(io.StringIO(decompressed_data), **kwargs)

def _decompress_gcsv_to_memory(gcsv_file: str) -> str:
    """
    Decompress the GCSV file into a string, without writing it to disk.
    This function reads the compressed chunks and decompresses them in memory.
    """
    decompressed_data = []

    with open(gcsv_file, 'rb') as f:
        while True:
            size_data = f.read(4)  # Read the chunk header (4 bytes)
            if not size_data:
                break  # No more chunks to read

            chunk_size = int.from_bytes(size_data, 'big')  # Read the chunk size
            compressed_chunk = f.read(chunk_size)  # Read the compressed chunk

            # Decompress the chunk and append to the result
            decompressed_data.append(zlib.decompress(compressed_chunk).decode('utf-8'))

    return ''.join(decompressed_data)

def to_gcsv(df: pd.DataFrame, gcsv_file: str, chunk_size=10, max_threads=16, **kwargs):
    """
    Write a pandas DataFrame to a GCSV file.
    :param df: pandas DataFrame to write.
    :param gcsv_file: Path to the output GCSV file.
    :param chunk_size: Chunk size in MB for compression.
    :param max_threads: Number of threads for compression.
    :param kwargs: Additional arguments passed to DataFrame.to_csv.
    """
    # Write the DataFrame to a memory buffer (in CSV format)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, **kwargs)

    # Now compress the CSV buffer content directly to the GCSV file
    csv_data = csv_buffer.getvalue()
    gcsv_compress_from_memory(csv_data, gcsv_file, chunk_size=chunk_size, max_threads=max_threads)

def gcsv_compress_from_memory(csv_data: str, gcsv_file: str, chunk_size=10, max_threads=16):
    """
    Compress CSV data directly from memory and write to the GCSV file.
    :param csv_data: CSV data as a string.
    :param gcsv_file: Path to the output GCSV file.
    :param chunk_size: Chunk size for compression.
    :param max_threads: Number of threads for compression.
    """
    with open(gcsv_file, 'wb') as f_out:
        # Split the data into chunks for compression
        chunk_size_bytes = chunk_size * 1024 * 1024  # Convert MB to bytes
        num_chunks = (len(csv_data) + chunk_size_bytes - 1) // chunk_size_bytes  # Number of chunks

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for i in range(num_chunks):
                start = i * chunk_size_bytes
                end = min((i + 1) * chunk_size_bytes, len(csv_data))
                chunk = csv_data[start:end]
                futures.append(executor.submit(compress_chunk, chunk))

            # Process the compressed data chunks and write them to the output file
            for future in futures:
                compressed_chunk = future.result()
                f_out.write(len(compressed_chunk).to_bytes(4, 'big'))  # Write chunk header
                f_out.write(compressed_chunk)  # Write chunk data

def compress_chunk(chunk: str) -> bytes:
    """Compress a single chunk of data."""
    return zlib.compress(chunk.encode('utf-8'))
