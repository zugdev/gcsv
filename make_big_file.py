import csv
import argparse
import random

def generate_csv(file_path, size_in_mb):
    words = [
        'apple', 'banana', 'cherry', 'date', 'elderberry', 'fig', 'grape', 'honeydew', 'kiwi', 'lemon',
        'mango', 'nectarine', 'orange', 'papaya', 'quince', 'raspberry', 'strawberry', 'tangerine', 'ugli', 'vanilla',
        'watermelon', 'xigua', 'yellow', 'zucchini', 'apricot', 'blackberry', 'cantaloupe', 'dragonfruit', 'eggplant', 'feijoa',
        'guava', 'huckleberry', 'imbe', 'jackfruit', 'kumquat', 'lime', 'mulberry', 'nutmeg', 'olive', 'peach',
        'quinoa', 'rambutan', 'sapodilla', 'tamarind', 'ugni', 'voavanga', 'wolfberry', 'ximenia', 'yam', 'ziziphus'
    ]

    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Column1', 'Column2', 'Column3']) 

        def generate_random_row():
            return [random.choice(words) for _ in range(3)]

        row = generate_random_row()
        row_size = sum(len(item) for item in row) + len(row) - 1  # approximate size of one row in bytes

        num_rows = (size_in_mb * 1024 * 1024) // row_size

        for _ in range(num_rows):
            row = generate_random_row()
            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate a CSV file of specified size.')
    parser.add_argument('output_file', type=str, help='The output CSV file name')
    parser.add_argument('size_in_mb', type=int, help='The size of the CSV file in megabytes')
    
    args = parser.parse_args()
    
    generate_csv(args.output_file, args.size_in_mb)
    print(f"CSV file '{args.output_file}' generated with size {args.size_in_mb} MB")