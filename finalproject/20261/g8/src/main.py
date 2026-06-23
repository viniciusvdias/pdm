import pandas as pd
import time
import statistics
import os
import shutil

# Caminho do arquivo CSV de entrada dentro do container Docker
INPUT_FILE = "/app/datasample/central_west.csv"

def convert_to_parquet(chunk_size):
    # Diretório de saída específico para cada tamanho de chunk
    output_dir = f"/app/data/output_parquet_{chunk_size}"

    # Remove saída anterior para garantir que não haja interferência nos resultados
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    # Cria o diretório de saída
    os.makedirs(output_dir)

    # Lê o CSV em modo incremental (chunk-by-chunk)
    reader = pd.read_csv(
        INPUT_FILE,
        chunksize=chunk_size
    )

    # Itera sobre cada chunk do dataset
    for i, chunk in enumerate(reader):
        # Converte cada chunk para Parquet e salva separadamente
        chunk.to_parquet(
            f"{output_dir}/part_{i}.parquet",
            index=False
        )

def get_folder_size(path):
    # Calcula o tamanho total dos arquivos dentro de um diretório

    total_size = 0

    # Percorre todas as pastas e arquivos dentro do caminho
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)

    return total_size

# Lista de tamanhos de chunk que serão testados no experimento
chunk_sizes = [10000, 50000, 100000]

print("\nRESULTADOS DOS EXPERIMENTOS")
print("=" * 60)

# Loop principal de experimentos para cada chunk size
for chunk_size in chunk_sizes:

    print(f"\nChunk Size: {chunk_size}")
    print("-" * 40)

    times = []  # Armazena os tempos de execução

    # Executa cada configuração 5 vezes para obter média estatística
    for execution in range(5):

        # Marca início da execução
        start = time.time()

        # Executa pipeline de conversão
        convert_to_parquet(chunk_size)

        # Calcula tempo total da execução
        duration = time.time() - start
        times.append(duration)

        print(
            f"Execução {execution + 1}: "
            f"{duration:.2f}s"
        )

    # Calcula métricas estatísticas
    average = statistics.mean(times)
    std_dev = statistics.stdev(times)

    # Caminho do output gerado
    output_dir = f"/app/data/output_parquet_{chunk_size}"

    # Calcula tamanho total dos arquivos gerados
    parquet_size_bytes = get_folder_size(output_dir)
    parquet_size_mb = parquet_size_bytes / (1024 * 1024)

    # Exibe resumo dos resultados
    print("\nResumo:")
    print(f"Média: {average:.2f}s")
    print(f"Desvio Padrão: {std_dev:.2f}s")
    print(f"Tamanho Parquet: {parquet_size_mb:.2f} MB")

print("\nExperimentos finalizados.")