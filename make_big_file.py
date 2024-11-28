import csv
import argparse
import random
import string

def generate_csv(file_path, size_in_mb, randomize):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Column1', 'Column2', 'Column3']) 

        def generate_random_row():
            return [''.join(random.choices(string.ascii_letters + string.digits, k=5)) for _ in range(3)]

        row = generate_random_row() if randomize else ['Data1', 'Data2', 'Data3']
        row_size = sum(len(item) for item in row) + len(row) - 1  # approximate size of one row in bytes

        num_rows = (size_in_mb * 1024 * 1024) // row_size

        for _ in range(num_rows):
            row = generate_random_row() if randomize else ['Data1', 'Data2', 'Data3']
            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate a CSV file of specified size.')
    parser.add_argument('output_file', type=str, help='The output CSV file name')
    parser.add_argument('size_in_mb', type=int, help='The size of the CSV file in megabytes')
    parser.add_argument('--random', action='store_true', help='Generate random characters in the CSV file')
    
    args = parser.parse_args()
    
    generate_csv(args.output_file, args.size_in_mb, args.random)
    print(f"CSV file '{args.output_file}' generated with size {args.size_in_mb} MB")