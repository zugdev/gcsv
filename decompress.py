# decompress.py
import zlib
import argparse
import threading
from queue import Queue

CHUNK_SIZE_BYTES = 4  # (chunk_header as defined in compress.py) 4 bytes to store the effective size of each compressed chunk

def decompress_chunk(chunk_data, chunk_index, output_queue):
    """
    Decompress a chunk of data and store the result in the queue.
    The queue ensures the chunks are written in the correct order.
    """
    decompressed_data = zlib.decompress(chunk_data)  # Decompress the chunk
    output_queue.put((chunk_index, decompressed_data))  # Store in the queue with its index

def read_chunks(input_file):
    """
    Generator to read compressed chunks from the input GCSV file.
    Each chunk is read with a 4-byte header that specifies the chunk's size.
    """
    with open(input_file, 'rb') as f_in:
        chunk_index = 0  # Track the order of chunks

        # Read the file until no more chunks are found
        while True:
            chunk_header = f_in.read(CHUNK_SIZE_BYTES)
            if not chunk_header:
                break

            # Convert the header to an integer
            chunk_size = int.from_bytes(chunk_header, 'big')

            # Read the compressed chunk data for chunk_size bytes
            chunk_data = f_in.read(chunk_size)

            # Yield the chunk index and the chunk data to caller (don't iterate through it yet)
            yield chunk_index, chunk_data

            # Increment the chunk index for the next chunk
            chunk_index += 1

def gcsv_decompress(input_file, output_file):
    """
    Decompress the GCSV file using multiple threads to speed up the process.
    Ensure the decompressed chunks are written sequentially to the output file.
    """
    output_queue = Queue()  # "Priority by index" queue to store decompressed chunks for ordered writing
    threads = []

    # Launch threads to decompress each chunk in parallel
    for chunk_index, chunk_data in read_chunks(input_file):
        thread = threading.Thread(
            target=decompress_chunk,
            args=(chunk_data, chunk_index, output_queue)  # Pass chunk data and queue
        )
        thread.start() 
        threads.append(thread)  # Add the thread to list

    # Thread endpoint join
    for thread in threads:
        thread.join()

    with open(output_file, 'wb') as f_out:
        # Create an empty list to store the decompressed chunks in proper order
        results = [None] * len(threads)

        # Extract decompressed chunks from the queue and store them by index
        while not output_queue.empty():
            chunk_index, decompressed_data = output_queue.get()
            results[chunk_index] = decompressed_data  # Store in the correct position
            print(f"decompressed chunk {chunk_index}: {len(decompressed_data)} bytes")

        # Write the decompressed chunks sequentially to the output file
        for decompressed_data in results:
            f_out.write(decompressed_data)
    print("done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Decompress a GCSV file back to CSV.")
    parser.add_argument("input_file", help="Path to the input compressed GCSV file. (i.e bitcoin.gcsv)")
    parser.add_argument("output_file", help="Path to the output CSV file. (i.e bitcoin.csv)")
    args = parser.parse_args()

    gcsv_decompress(args.input_file, args.output_file)