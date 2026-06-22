import pandas as pd
import time
import statistics
import os

def convert_to_parquet():
    chunk_size = 100000

    input_file = '/app/central_west.csv'
    output_dir = '/app/data/output_parquet'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    reader = pd.read_csv(input_file, chunksize=chunk_size)
    
    for i, chunk in enumerate(reader):
        chunk.to_parquet(f'{output_dir}/part_{i}.parquet', index=False)

# Benchmark
times = []
for i in range(10):
    start = time.time()
    convert_to_parquet()
    duration = time.time() - start
    times.append(duration)
    
    #tempo de cada execução individual
    print(f"Execução {i+1}: {duration:.2f}s")

print("-" * 20)
print(f"Média: {statistics.mean(times):.2f}s")
print(f"Desvio Padrão: {statistics.stdev(times):.2f}s")