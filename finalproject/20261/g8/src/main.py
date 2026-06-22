import pandas as pd
import time
import statistics
import os
import shutil

INPUT_FILE = "/app/central_west.csv"

def convert_to_parquet(chunk_size):
    output_dir = f"/app/data/output_parquet_{chunk_size}"

    # Remove saída anterior para evitar influência de arquivos já existentes
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    os.makedirs(output_dir)

    reader = pd.read_csv(
        INPUT_FILE,
        chunksize=chunk_size
    )

    for i, chunk in enumerate(reader):
        chunk.to_parquet(
            f"{output_dir}/part_{i}.parquet",
            index=False
        )

def get_folder_size(path):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)

    return total_size

# Configurações a comparar
chunk_sizes = [10000, 50000, 100000]

print("\nRESULTADOS DOS EXPERIMENTOS")
print("=" * 60)

for chunk_size in chunk_sizes:

    print(f"\nChunk Size: {chunk_size}")
    print("-" * 40)

    times = []

    for execution in range(5):

        start = time.time()

        convert_to_parquet(chunk_size)

        duration = time.time() - start
        times.append(duration)

        print(
            f"Execução {execution + 1}: "
            f"{duration:.2f}s"
        )

    average = statistics.mean(times)
    std_dev = statistics.stdev(times)

    output_dir = f"/app/data/output_parquet_{chunk_size}"

    parquet_size_bytes = get_folder_size(output_dir)
    parquet_size_mb = parquet_size_bytes / (1024 * 1024)

    print("\nResumo:")
    print(f"Média: {average:.2f}s")
    print(f"Desvio Padrão: {std_dev:.2f}s")
    print(f"Tamanho Parquet: {parquet_size_mb:.2f} MB")

print("\nExperimentos finalizados.")