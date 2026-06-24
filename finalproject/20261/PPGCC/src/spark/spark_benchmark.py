"""
spark_benchmark.py — mesmo perfilamento do DataSetSummary, implementado em PySpark.

Objetivo: comparar PySpark (local[N]) com ProcessPoolExecutor no mesmo dataset.
A diferença principal é que Spark usa threads Java (sem GIL) e o Catalyst Optimizer
compila as agregações em um único job que lê cada linha uma única vez.

Variáveis de ambiente:
  DATASET_PATH   caminho do CSV dentro do container (padrão: /datasets/taxi.csv)
  CORES_LIST     lista de núcleos separada por vírgula (padrão: 1,2,4,8,16)
  N_REPS         repetições por configuração (padrão: 3)
  RESULTS_PATH   caminho de saída do CSV (padrão: /results/metrics_spark.csv)
  DRIVER_MEM     memória do driver Spark (padrão: 4g)
"""

import os
import csv
import re
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SENSITIVE_RE = re.compile(r"login|cpf|cnpj", re.IGNORECASE)


def create_session(n_cores: int) -> SparkSession:
    """Cria uma SparkSession em modo local com n_cores threads."""
    driver_mem = os.environ.get("DRIVER_MEM", "4g")
    return (
        SparkSession.builder
        .appName(f"TaxiProfiler-local[{n_cores}]")
        .master(f"local[{n_cores}]")
        # Shuffle partitions: mínimo 4 para evitar gargalo em poucos núcleos
        .config("spark.sql.shuffle.partitions", str(max(n_cores * 2, 4)))
        .config("spark.driver.memory", driver_mem)
        # Adaptive Query Execution: reotimiza planos em tempo real
        .config("spark.sql.adaptive.enabled", "true")
        # Compressão de broadcast desligada para benchmark justo
        .config("spark.broadcast.compress", "false")
        # UI disponível em localhost:4040 enquanto o job roda
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )


def profile_spark(spark: SparkSession, csv_path: str) -> dict:
    """
    Replica a lógica do DataSetSummary usando a API PySpark DataFrame.

    Diferenças em relação ao profiler.py (multiprocessing):
    - Leitura: Spark lê em blocos internos; o Catalyst otimiza o plano de execução
    - Agregações numéricas: um único job Spark computa min/max/mean/std de todas as
      colunas ao mesmo tempo (scan único)
    - Cardinalidade: usa approx_count_distinct (HyperLogLog) — mais rápido que nunique
      exato, com erro < 5%
    - value_counts: um groupBy por coluna categórica (separado)
    - Threads Java: não sofrem bloqueio do GIL do Python
    """

    # 1. Leitura e filtragem de colunas sensíveis
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    safe_cols = [c for c in df.columns if not SENSITIVE_RE.search(c)]
    df = df.select(safe_cols)

    n_cols = len(df.columns)

    # 2. Contagem de linhas + nulos em um único job (Action: collect)
    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    null_exprs.append(F.count(F.lit(1)).alias("__n_rows__"))
    base_row = df.agg(*null_exprs).collect()[0]
    n_rows = base_row["__n_rows__"]

    # 3. Estatísticas numéricas — um único job para todas as colunas
    numeric_cols = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString() in ("int", "bigint", "float", "double")
    ]
    if numeric_cols:
        num_exprs = []
        for col in numeric_cols:
            num_exprs += [
                F.min(col).alias(f"min__{col}"),
                F.max(col).alias(f"max__{col}"),
                F.mean(col).alias(f"mean__{col}"),
                F.stddev(col).alias(f"std__{col}"),
                # approx_count_distinct usa HyperLogLog (± 5% de erro, muito mais rápido)
                F.approx_count_distinct(col, rsd=0.05).alias(f"nuniq__{col}"),
            ]
        df.agg(*num_exprs).collect()

    # 4. Colunas string: detecção de data + value_counts para baixa cardinalidade
    string_cols = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString() == "string"
    ]
    for col in string_cols:
        # Amostra para detecção de data
        sample_row = df.select(col).dropna().first()
        if sample_row:
            s = str(sample_row[0])
            has_sep   = any(sep in s for sep in ['/', '-', ' '])
            has_digit = any(c.isdigit() for c in s)
            if has_sep and has_digit:
                continue  # coluna de data — pula o value_counts

        n_unique = df.select(F.approx_count_distinct(col, rsd=0.05)).collect()[0][0]
        if n_unique <= 50:
            # value_counts equivalente
            df.groupBy(col).count().orderBy("count", ascending=False).collect()

    return {"n_rows": n_rows, "n_cols": n_cols}


def run_experiment(spark: SparkSession, csv_path: str):
    """Mede o tempo de um perfilamento completo. Retorna (elapsed, throughput, size_mb)."""
    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    start = time.perf_counter()
    profile_spark(spark, csv_path)
    elapsed = time.perf_counter() - start
    return elapsed, file_size_mb / elapsed, file_size_mb


def main():
    dataset_path = os.environ.get("DATASET_PATH", "/datasets/taxi.csv")
    cores_raw    = os.environ.get("CORES_LIST",   "1,2,4,8,16")
    cores_list   = [int(c.strip()) for c in cores_raw.split(",")]
    n_reps       = int(os.environ.get("N_REPS", "3"))
    results_path = os.environ.get("RESULTS_PATH", "/results/metrics_spark.csv")

    os.makedirs(os.path.dirname(results_path), exist_ok=True)

    file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
    print(f"Dataset : {dataset_path}  ({file_size_mb:.1f} MB)", flush=True)
    print(f"Núcleos : {cores_list}", flush=True)
    print(f"Reps    : {n_reps}", flush=True)
    print("-" * 60, flush=True)

    fieldnames = ["method", "dataset_size_mb", "n_workers", "rep", "time_seconds", "throughput_mb_s"]

    with open(results_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for n_cores in cores_list:
            print(f"\n=== Spark local[{n_cores}] ===", flush=True)
            spark = create_session(n_cores)

            # Warmup: primeira execução aquece a JVM e o cache do Catalyst
            print("  [warmup] ...", flush=True)
            run_experiment(spark, dataset_path)

            for rep in range(1, n_reps + 1):
                print(f"  [local[{n_cores}]] rep {rep}/{n_reps} ...", flush=True)
                elapsed, throughput, size_mb = run_experiment(spark, dataset_path)
                print(f"    → {elapsed:.2f}s  |  {throughput:.2f} MB/s", flush=True)
                writer.writerow({
                    "method": "spark",
                    "dataset_size_mb": f"{size_mb:.1f}",
                    "n_workers": n_cores,
                    "rep": rep,
                    "time_seconds": f"{elapsed:.4f}",
                    "throughput_mb_s": f"{throughput:.4f}",
                })
                f.flush()

            spark.stop()

    print("-" * 60, flush=True)
    print("Concluído. Resultados salvos.", flush=True)


if __name__ == "__main__":
    main()
