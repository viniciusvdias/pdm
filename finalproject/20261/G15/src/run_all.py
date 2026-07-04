"""
Orquestrador do pipeline: roda as etapas na ordem correta, cada uma em um PROCESSO SEPARADO.

Processos separados de proposito: garante que o benchmark do .jpg (02) e o do parquet (04)
comecem com o page-cache/estado do TF limpos, sem contaminacao de uma etapa na outra.

Ordem: [01 preparo] -> 02 baseline jpg -> 03 ETL Spark -> 04 parquet + comparacao.
  - 02 le RAW_DIR (.jpg);  03 gera PARQUET_DIR a partir de RAW_DIR;  04 le PARQUET_DIR
    e compara com o stats do 02.
  - 01 (preparo/duplicacao) so roda se RUN_PREPARE=1 (fluxo do dataset completo).

Config toda via env (ver bench_common.py). Uso: python run_all.py
"""

import os
import subprocess
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent

STEPS = ["02_baseline_jpg.py", "03_spark_etl.py", "04_optimized_parquet.py"]
if os.environ.get("RUN_PREPARE") == "1":
    STEPS = ["01_download.py"] + STEPS


def run(script):
    print(f"\n{'='*70}\n>>> {script}\n{'='*70}", flush=True)
    # cwd=SRC_DIR para que os scripts achem o bench_common; herda o env (config do experimento).
    result = subprocess.run([sys.executable, script], cwd=SRC_DIR)
    if result.returncode != 0:
        print(f"\n[X] {script} falhou (exit {result.returncode}). Abortando.", flush=True)
        sys.exit(result.returncode)


if __name__ == "__main__":
    print("Pipeline:", " -> ".join(STEPS))
    print("RESULTS_DIR:", os.environ.get("RESULTS_DIR", "(default /tf/data/results)"))
    for step in STEPS:
        run(step)
    print(f"\n{'='*70}\nPipeline completo. Resultados (CSVs + PNGs) em RESULTS_DIR.\n{'='*70}")
