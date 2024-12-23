# gcsv

A **compressed CSV file format** built on **zlib** that leverages **concurrent computing** for fast compression and decompression. It is bundled with **Python tooling** to enable native use in data science projects.

## Requirements

Currently, there are **no external dependencies**! The project relies entirely on **Python’s standard libraries**. However, we plan to release some tooling as a **PyPI package** in the future to streamline its use.

---

## Usage

### **compress.py** – Compressing Files

Compression is **chunk-based**. We split an input CSV file into **N/M chunks** of **M bytes** each, where each chunk is processed by a **pool of threads** in parallel. The compressed result of each chunk is stored in a **futures array** to ensure it can be written sequentially. 

Each compressed chunk is preceded by a **4-byte header** that stores the size of the chunk. This header ensures the correct size is known during decompression.

#### Example Usage:
```shell
python compress.py [input-file] [output-file] 
# Example:
python compress.py mnist.csv mnist.gcsv
```

#### View Help:
```shell
python compress.py -h
```

---

### **decompress.py** – Decompressing Files

During decompression, we **read each chunk sequentially** by first reading the **4-byte header** to determine the chunk size. 

Example of chunk reading logic:
```python
# Read the file until no more chunks are found
while True:
    chunk_header = f_in.read(CHUNK_SIZE_BYTES)
    if not chunk_header:
        break

    # Convert the 4-byte header into an integer (chunk size)
    chunk_size = int.from_bytes(chunk_header, 'big')

    # Read the compressed chunk data for the given size
    chunk_data = f_in.read(chunk_size)
```

Each chunk is **decompressed concurrently** using multiple threads, and results are stored in a **priority queue** to maintain the original order during reconstruction.

#### Example Usage:
```shell
python decompress.py [input-file] [output-file] 
# Example:
python decompress.py mnist.gcsv mnist.csv
```

---

### **correctness_test.py** – Verifying Compression and Decompression

This script takes an **input CSV file**, compresses it, and then decompresses it. The decompressed file is **diffed against the original** to ensure **lossless compression**. If the files are identical, the test passes.

#### Example Usage:
```shell
python correctness_test.py [input-file] 
# Example:
python correctness_test.py mnist.csv
```

#### Example Output:
```
[TEST PASSED] The input and decompressed files are identical.
```

## Performance

Our default arguments of 10Mbs chunks and 16 max threads for the Executor were empirically obtained. You can find the optimal arguments for a specific file by running `compress_tests.py` with it as input. Here are the plotted graphs against `msint.csv`:

![Compression Time vs Chunk Size (Original File Size: 92.36 MB)](compression-time_MINST.png)

![Compression Ratio vs Chunk Size (Original File Size: 92.36 MB)](compression-ratio_MINST.png)