import os
import time
import matplotlib.pyplot as plt
from compress import gcsv_compress

def test_compression(input_file, output_file, chunk_sizes, max_threads_list):
    """
    Test different chunk sizes and thread counts to evaluate compression performance and efficiency.
    :param input_file: Path to the input CSV file.
    :param output_file: Path to the output GCSV file.
    :param chunk_sizes: List of chunk sizes (in MB) to test.
    :param max_threads_list: List of max thread counts to test.
    :return: Results dictionary containing metrics for each combination.
    """
    results = []

    for chunk_size in chunk_sizes:
        for max_threads in max_threads_list:
            print(f"Testing with chunk_size={chunk_size} MB, max_threads={max_threads}")

            # Measure time and file size
            start_time = time.time()
            gcsv_compress(input_file, output_file, chunk_size, max_threads)
            end_time = time.time()

            # Get compressed file size
            compressed_size = os.path.getsize(output_file)

            # Append results
            results.append({
                'chunk_size': chunk_size,
                'max_threads': max_threads,
                'time_cost': end_time - start_time,
                'compressed_size': compressed_size
            })

            # Clean up the output file to save space
            os.remove(output_file)

    return results

def plot_results(results, input_file_size):
    """
    Plot the results of compression tests.
    :param results: List of results from the test_compression function.
    :param input_file_size: Size of the original input file in bytes.
    """
    # Prepare data for plotting
    chunk_sizes = sorted(set(result['chunk_size'] for result in results))
    max_threads_list = sorted(set(result['max_threads'] for result in results))
    
    # Plot time cost
    for max_threads in max_threads_list:
        times = [result['time_cost'] for result in results if result['max_threads'] == max_threads]
        plt.plot(chunk_sizes, times, label=f"{max_threads} threads")

    plt.title(f"Compression Time vs Chunk Size (Original File Size: {input_file_size / (1024 * 1024):.2f} MB)")
    plt.xlabel("Chunk Size (MB)")
    plt.ylabel("Time Cost (seconds)")
    plt.legend()
    plt.show()

    # Plot compression ratio
    for max_threads in max_threads_list:
        compression_ratios = [
            100 * (1 - result['compressed_size'] / input_file_size)
            for result in results if result['max_threads'] == max_threads
        ]
        plt.plot(chunk_sizes, compression_ratios, label=f"{max_threads} threads")

    plt.title(f"Compression Ratio vs Chunk Size (Original File Size: {input_file_size / (1024 * 1024):.2f} MB)")
    plt.xlabel("Chunk Size (MB)")
    plt.ylabel("Compression Ratio (%)")
    plt.legend()
    plt.show()

if __name__ == "__main__":
    INPUT_FILE = "mnist.csv" 
    OUTPUT_FILE = "compressed.gcsv"  # Ephemeral file
    CHUNK_SIZES = [1, 2, 5, 10, 20, 30, 40, 50]  # Test chunk sizes in MB
    MAX_THREADS_LIST = [1, 2, 4, 8, 16, 32, 64, 128]  # Test different thread counts

    input_file_size = os.path.getsize(INPUT_FILE)

    results = test_compression(INPUT_FILE, OUTPUT_FILE, CHUNK_SIZES, MAX_THREADS_LIST)

    plot_results(results, input_file_size)
