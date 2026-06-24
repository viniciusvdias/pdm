"""
benchmark.py — executor de experimentos de perfilamento paralelo com registro de métricas.

Variáveis de ambiente:
  DATASET_PATH   caminho para o CSV dentro do container (padrão: /datasets/taxi.csv)
  WORKERS_LIST   lista de quantidades de workers separada por vírgulas (padrão: 1,2,4,8,16)
  N_REPS         número de repetições por configuração (padrão: 3)
  CHUNK_SIZE     linhas por chunk (padrão: 500000)
  RESULTS_PATH   caminho de saída do CSV de métricas (padrão: /results/metrics.csv)

Saída: results/metrics.csv com colunas:
  dataset_size_mb, n_workers, rep, time_seconds, throughput_mb_s
"""

import os
import csv
import time
import concurrent.futures

import pandas as pd

from profiler import profile_chunk
from aggregator import aggregate_chunks, format_summary


def _run_serial(reader_factory, chunk_size: int) -> list:
    """Executa o perfilamento em loop simples (sem overhead de executor paralelo)."""
    partial_results = []
    for chunk_id, chunk in enumerate(reader_factory(chunk_size)):
        partial_results.append(profile_chunk((chunk, chunk_id)))
    return partial_results


def _run_parallel(reader_factory, chunk_size: int, n_workers: int) -> list:
    """
    Executa o perfilamento com ProcessPoolExecutor.

    Usa janela deslizante (máx. 2×n_workers chunks em voo ao mesmo tempo) para evitar
    carregar o dataset inteiro na RAM simultaneamente.
    """
    MAX_PENDING = n_workers * 2
    partial_results = []
    pending: dict = {}

    with concurrent.futures.ProcessPoolExecutor(max_workers=n_workers) as executor:
        for chunk_id, chunk in enumerate(reader_factory(chunk_size)):
            future = executor.submit(profile_chunk, (chunk, chunk_id))
            pending[future] = chunk_id

            # Drena a fila quando a janela está cheia para liberar memória
            while len(pending) >= MAX_PENDING:
                done_fut = next(concurrent.futures.as_completed(pending))
                partial_results.append(done_fut.result())
                del pending[done_fut]

        # Drena os futures restantes ao final do dataset
        for fut in concurrent.futures.as_completed(pending):
            partial_results.append(fut.result())

    return partial_results


def run_experiment(dataset_path: str, n_workers: int, chunk_size: int):
    """
    Executa um experimento completo de perfilamento.

    n_workers == 1  → loop serial (sem overhead do ProcessPoolExecutor)
    n_workers >= 2  → paralelo com ProcessPoolExecutor

    Retorna (elapsed_seconds, throughput_mb_s, file_size_mb, summary_text).
    """
    file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)

    def reader_factory(cs):
        return pd.read_csv(dataset_path, chunksize=cs, low_memory=False)

    start = time.perf_counter()

    if n_workers == 1:
        partial_results = _run_serial(reader_factory, chunk_size)
    else:
        partial_results = _run_parallel(reader_factory, chunk_size, n_workers)

    # Combina todos os resultados parciais dos chunks em um resumo final
    agg = aggregate_chunks(partial_results)
    summary = format_summary(agg)

    elapsed = time.perf_counter() - start
    throughput = file_size_mb / elapsed if elapsed > 0 else 0.0

    return elapsed, throughput, file_size_mb, summary


def main():
    # Lê configurações das variáveis de ambiente (permite sobrescrever via Docker Compose)
    dataset_path = os.environ.get("DATASET_PATH", "/datasets/taxi.csv")
    workers_raw = os.environ.get("WORKERS_LIST", "1,2,4,8,16")
    workers_list = [int(w.strip()) for w in workers_raw.split(",")]
    n_reps = int(os.environ.get("N_REPS", "3"))
    chunk_size = int(os.environ.get("CHUNK_SIZE", "500000"))
    results_path = os.environ.get("RESULTS_PATH", "/results/metrics.csv")

    os.makedirs(os.path.dirname(results_path), exist_ok=True)

    file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
    print(f"Dataset: {dataset_path}  ({file_size_mb:.1f} MB)", flush=True)
    print(f"Workers a testar: {workers_list}", flush=True)
    print(f"Repetições por configuração: {n_reps}", flush=True)
    print(f"Tamanho do chunk: {chunk_size:,} linhas", flush=True)
    print(f"Resultados → {results_path}", flush=True)
    print("-" * 60, flush=True)

    fieldnames = ["dataset_size_mb", "n_workers", "rep", "time_seconds", "throughput_mb_s"]

    with open(results_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for n_workers in workers_list:
            for rep in range(1, n_reps + 1):
                label = "serial" if n_workers == 1 else f"{n_workers} workers"
                print(f"  [{label}] rep {rep}/{n_reps} ...", flush=True)
                elapsed, throughput, size_mb, _ = run_experiment(
                    dataset_path, n_workers, chunk_size
                )
                print(f"    → {elapsed:.2f}s  |  {throughput:.2f} MB/s", flush=True)
                writer.writerow({
                    "dataset_size_mb": f"{size_mb:.1f}",
                    "n_workers": n_workers,
                    "rep": rep,
                    "time_seconds": f"{elapsed:.4f}",
                    "throughput_mb_s": f"{throughput:.4f}",
                })
                f.flush()  # garante que cada linha é gravada imediatamente no disco

    print("-" * 60, flush=True)
    print("Concluído. Resultados salvos.", flush=True)


# Guarda obrigatória para multiprocessing no Windows — novos processos reimportam este módulo
if __name__ == "__main__":
    main()
