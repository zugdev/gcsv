import pandas_gcsv
import pandas as pd
import time
import matplotlib.pyplot as plt
import psutil

# times process and measure RAM usage (runs 25 times)
def time_and_memory_process(func, *args, **kwargs):
    times = []
    memory_usages = []
    for _ in range(25): 
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss  # Memory before execution
        func(*args, **kwargs)
        end_time = time.time()
        end_memory = process.memory_info().rss  # Memory after execution
        times.append(end_time - start_time)
        memory_usages.append(end_memory - start_memory)
    return times, memory_usages

def process_gcsv():
    df = pandas_gcsv.read_gcsv('mnist.gcsv')            # Read GCSV file
    df['label'] = 9                                     # Modify column
    pandas_gcsv.to_gcsv(df, 'modified.gcsv')            # Write to GCSV file

def process_csv():
    df = pd.read_csv('mnist.csv')                       # Read CSV file
    df['label'] = 9                                     # Modify column
    df.to_csv('modified.csv', index=False)              # Write to CSV file

# time GCSV process and measure RAM usage
print("Timing GCSV process and measuring RAM usage...")
gcsv_times, gcsv_memory_usages = time_and_memory_process(process_gcsv)
gcsv_avg_time = sum(gcsv_times) / len(gcsv_times)
gcsv_avg_memory = sum(gcsv_memory_usages) / len(gcsv_memory_usages)
gcsv_max_memory = max(gcsv_memory_usages) / (1024 * 1024)  # Max memory in MB
gcsv_min_memory = min(gcsv_memory_usages) / (1024 * 1024)  # Min memory in MB
print(f"Average GCSV process time: {gcsv_avg_time:.4f} seconds")
print(f"Average GCSV memory usage: {gcsv_avg_memory / (1024 * 1024):.2f} MB")
print(f"Max GCSV memory usage: {gcsv_max_memory:.2f} MB")
print(f"Min GCSV memory usage: {gcsv_min_memory:.2f} MB")

# time the CSV process and measure RAM usage
print("Timing CSV process and measuring RAM usage...")
csv_times, csv_memory_usages = time_and_memory_process(process_csv)
csv_avg_time = sum(csv_times) / len(csv_times)
csv_avg_memory = sum(csv_memory_usages) / len(csv_memory_usages)
csv_max_memory = max(csv_memory_usages) / (1024 * 1024)  # Max memory in MB
csv_min_memory = min(csv_memory_usages) / (1024 * 1024)  # Min memory in MB
print(f"Average CSV process time: {csv_avg_time:.4f} seconds")
print(f"Average CSV memory usage: {csv_avg_memory / (1024 * 1024):.2f} MB")
print(f"Max CSV memory usage: {csv_max_memory:.2f} MB")
print(f"Min CSV memory usage: {csv_min_memory:.2f} MB")

# plot the average execution times
plt.figure(figsize=(10, 6))

# plot execution times
plt.subplot(2, 1, 1)
plt.bar(['GCSV', 'CSV'], [gcsv_avg_time, csv_avg_time], color=['blue', 'orange'])
plt.ylabel('Time (seconds)')
plt.title('Average GCSV vs CSV Processing Time')

# plot memory usage
plt.subplot(2, 1, 2)
plt.bar(['GCSV', 'CSV'], [gcsv_avg_memory / (1024 * 1024), csv_avg_memory / (1024 * 1024)], color=['blue', 'orange'])
plt.ylabel('Memory Usage (MB)')
plt.title('Average GCSV vs CSV Memory Usage')

plt.tight_layout()
plt.show()
