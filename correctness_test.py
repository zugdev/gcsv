from compress import gcsv_compress
from decompress import gcsv_decompress
import argparse
import os
import filecmp

def main():
    parser = argparse.ArgumentParser(description='Compress, decompress, and diff a file.')
    parser.add_argument('input_file', type=str, help='The input file to process (i.e bitcoin_data.csv)')
    args = parser.parse_args()

    input_file = args.input_file
    compressed_file = input_file + '.gcsv'
    decompressed_file = input_file + '_decompressed.csv'

    # Compress the input file
    gcsv_compress(input_file, compressed_file, 10, 16)

    # Decompress the file
    gcsv_decompress(compressed_file, decompressed_file)

    # Compare the original and decompressed files
    if filecmp.cmp(input_file, decompressed_file, shallow=False):
        print('[TEST PASSED] The input and decompressed files are identical.')
    else:
        print('[TEST FAILED] The input and decompressed files are different.')

    # Clean up
    os.remove(compressed_file)
    os.remove(decompressed_file)

if __name__ == '__main__':
    main()