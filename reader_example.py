from gcsv import GCSVReader

with GCSVReader('mnist.gcsv') as reader:
    for row in reader:
        print(row)