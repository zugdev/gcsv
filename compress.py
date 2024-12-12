# compress.py
import zlib
import argparse
from concurrent.futures import ThreadPoolExecutor

def compress_chunk(chunk):
    """Compress a single chunk of data."""
    return zlib.compress(chunk)

# We divide the file in chunks and then divide those chunks between threads to compress individually
def gcsv_compress(input_file, output_file, chunk_size=10, max_threads=16):
    """
    Split the input file into 1 MB chunks, compress each chunk using multiple threads,
    and write the compressed chunks sequentially to the output file.
    """
    # Open the input and output files in binary mode (important for linux/windows compatibility)
    with open(input_file, 'rb') as f_in, open(output_file, 'wb') as f_out:
        # Create a thread pool where threads will pick tasks from
        with ThreadPoolExecutor() as executor:
            print(f"compressing with {executor._max_workers} max threads and {chunk_size} MB chunks with 4 bytes header per compressed block")
            executor._max_workers = max_threads  # Set the maximum number of threads to use
            futures = []  # We'll store futures (results) for each chunk compression task for post sequential writing

            # Read the input file chunk by chunk until the end of the file
            while True:
                chunk = f_in.read(chunk_size * 1024 * 1024)
                if not chunk:
                    break

                # Delegate chunk compression to a thread and store the future
                futures.append(executor.submit(compress_chunk, chunk))

            # Iterate over the completed futures to get compressed data
            for future in futures:
                compressed_chunk = future.result()  # Get the compressed chunk

                print(f"compressing chunk {futures.index(future)}: {len(compressed_chunk)} bytes")
                
                # (chunk_header): Write the size of the compressed chunk (as 4 bytes (2^32 bits)) to the output file
                f_out.write(len(compressed_chunk).to_bytes(4, 'big'))

                # (chunk_data): Write the compressed chunk data to the output file
                f_out.write(compressed_chunk)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compress a CSV file into a compressed GCSV file.")
    parser.add_argument("input_file", help="Path to the input CSV file. (i.e bitcoin.csv)")
    parser.add_argument("output_file", help="Path to the output compressed GCSV file. (i.e bitcoin.gcsv)")
    parser.add_argument("--chunk-size", type=int, default=10, help="Size of the chunks in megabytes (Mbs) to read from the input file (i.e 10)")
    parser.add_argument("--max-threads", type=int, default=16, help="Maximum number of threads to use for compression (i.e 16)")
    args = parser.parse_args()

    gcsv_compress(args.input_file, args.output_file, args.chunk_size, args.max_threads)