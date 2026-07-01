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
    partial_results = []  # lista que vai acumulando o resultado de cada chunk

    # enumerate() entrega (0, chunk0), (1, chunk1), ... para rastrear a ordem
    for chunk_id, chunk in enumerate(reader_factory(chunk_size)):
        # processa o chunk e adiciona o resultado parcial na lista
        partial_results.append(profile_chunk((chunk, chunk_id)))

    return partial_results  # lista com um dict de estatísticas por chunk


def _run_parallel(reader_factory, chunk_size: int, n_workers: int) -> list:
    """
    Executa o perfilamento com ProcessPoolExecutor.

    Usa janela deslizante (máx. 2×n_workers chunks em voo ao mesmo tempo) para evitar
    carregar o dataset inteiro na RAM simultaneamente.
    """
    # limite de chunks simultâneos na RAM: 2× o número de workers
    # ex: 4 workers → máx 8 chunks voando ao mesmo tempo
    MAX_PENDING = n_workers * 2

    partial_results = []  # resultados já concluídos ficam aqui
    pending: dict = {}    # futures ainda em execução: {future: chunk_id}

    # ProcessPoolExecutor cria N processos filhos; cada um roda profile_chunk() em paralelo
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_workers) as executor:
        for chunk_id, chunk in enumerate(reader_factory(chunk_size)):
            # submete o chunk para um processo livre; retorna um "futuro" (promessa de resultado)
            future = executor.submit(profile_chunk, (chunk, chunk_id))
            pending[future] = chunk_id  # registra o futuro como pendente

            # janela deslizante: se a fila encheu, espera pelo menos um terminar antes de continuar
            while len(pending) >= MAX_PENDING:
                # as_completed() retorna o próximo futuro que concluir (qualquer ordem)
                done_fut = next(concurrent.futures.as_completed(pending))
                partial_results.append(done_fut.result())  # salva o resultado
                del pending[done_fut]                       # libera o slot na janela

        # fim do CSV: drena os futures que ainda estão rodando
        for fut in concurrent.futures.as_completed(pending):
            partial_results.append(fut.result())

    return partial_results  # lista com um dict de estatísticas por chunk (fora de ordem — o aggregator reordena)


def run_experiment(dataset_path: str, n_workers: int, chunk_size: int):
    """
    Executa um experimento completo de perfilamento.

    n_workers == 1  → loop serial (sem overhead do ProcessPoolExecutor)
    n_workers >= 2  → paralelo com ProcessPoolExecutor

    Retorna (elapsed_seconds, throughput_mb_s, file_size_mb, summary_text).
    """
    # tamanho do arquivo em MB — usado para calcular o throughput ao final
    file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)

    # reader_factory é uma função que cria um iterador de chunks do CSV
    # chunksize=cs → Pandas lê cs linhas por vez em vez de carregar tudo
    def reader_factory(cs):
        return pd.read_csv(dataset_path, chunksize=cs, low_memory=False)

    # marca o instante inicial com relógio de alta resolução (precisão de nanossegundos)
    start = time.perf_counter()

    # escolhe serial ou paralelo conforme o número de workers solicitado
    if n_workers == 1:
        partial_results = _run_serial(reader_factory, chunk_size)
    else:
        partial_results = _run_parallel(reader_factory, chunk_size, n_workers)

    # combina todos os resultados parciais dos chunks em um único dict agregado
    agg = aggregate_chunks(partial_results)

    # formata o dict agregado em texto legível (mesma saída do DataSetSummary original)
    summary = format_summary(agg)

    # tempo total decorrido desde o start até aqui (inclui leitura + processamento + agregação)
    elapsed = time.perf_counter() - start

    # throughput: quantos MB foram processados por segundo
    # ex: 500 MB em 10s → 50 MB/s
    throughput = file_size_mb / elapsed if elapsed > 0 else 0.0

    return elapsed, throughput, file_size_mb, summary


def main():
    # lê configurações das variáveis de ambiente (permite sobrescrever via Docker Compose ou terminal)
    dataset_path = os.environ.get("DATASET_PATH", "/datasets/taxi.csv")

    # WORKERS_LIST é uma string "1,2,4,8,16" → split por vírgula → lista de ints [1,2,4,8,16]
    workers_raw = os.environ.get("WORKERS_LIST", "1,2,4,8,16")
    workers_list = [int(w.strip()) for w in workers_raw.split(",")]

    n_reps = int(os.environ.get("N_REPS", "3"))          # repetições por configuração (para reduzir ruído)
    chunk_size = int(os.environ.get("CHUNK_SIZE", "500000"))  # linhas por chunk
    results_path = os.environ.get("RESULTS_PATH", "/results/metrics.csv")

    # cria o diretório de saída se não existir
    os.makedirs(os.path.dirname(results_path), exist_ok=True)

    file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
    print(f"Dataset: {dataset_path}  ({file_size_mb:.1f} MB)", flush=True)
    print(f"Workers a testar: {workers_list}", flush=True)
    print(f"Repetições por configuração: {n_reps}", flush=True)
    print(f"Tamanho do chunk: {chunk_size:,} linhas", flush=True)
    print(f"Resultados → {results_path}", flush=True)
    print("-" * 60, flush=True)

    # colunas do CSV de saída
    fieldnames = ["dataset_size_mb", "n_workers", "rep", "time_seconds", "throughput_mb_s"]

    with open(results_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()  # escreve a linha de cabeçalho no CSV

        # loop externo: cada configuração de workers (ex: 1, 2, 4, 8, 16)
        for n_workers in workers_list:
            # loop interno: repete N vezes para obter médias mais estáveis
            for rep in range(1, n_reps + 1):
                label = "serial" if n_workers == 1 else f"{n_workers} workers"
                print(f"  [{label}] rep {rep}/{n_reps} ...", flush=True)

                # roda o experimento; _ descarta o summary (não vai para o CSV)
                elapsed, throughput, size_mb, _ = run_experiment(
                    dataset_path, n_workers, chunk_size
                )
                print(f"    → {elapsed:.2f}s  |  {throughput:.2f} MB/s", flush=True)

                # grava uma linha no CSV com as métricas desta repetição
                writer.writerow({
                    "dataset_size_mb": f"{size_mb:.1f}",
                    "n_workers": n_workers,
                    "rep": rep,
                    "time_seconds": f"{elapsed:.4f}",
                    "throughput_mb_s": f"{throughput:.4f}",
                })
                f.flush()  # força gravação no disco agora (evita perda se o processo morrer)

    print("-" * 60, flush=True)
    print("Concluído. Resultados salvos.", flush=True)


# Guarda obrigatória para multiprocessing no Windows — novos processos reimportam este módulo
if __name__ == "__main__":
    main()
