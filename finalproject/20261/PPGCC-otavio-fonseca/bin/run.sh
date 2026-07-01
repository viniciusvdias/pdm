#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Helper para rodar o projeto out-of-the-box via Docker Compose.
# Requisito único: Docker instalado e rodando. Nenhuma outra dependência.
#
# Uso:
#   ./bin/run.sh sample      # roda o benchmark com a amostra (datasample/)  [padrão]
#   ./bin/run.sh spark        # roda o benchmark Spark com a amostra
#   ./bin/run.sh download     # baixa o dataset completo (1/5/10 GB) do NYC TLC
#   ./bin/run.sh full-1gb     # roda o benchmark Multiprocessing no dataset de 1 GB
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Diretório raiz do grupo (pai de bin/)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$ROOT/bigdata-preprocessing"
ACTION="${1:-sample}"

cd "$COMPOSE_DIR"

case "$ACTION" in
  sample)
    echo ">> Build da imagem de benchmark..."
    docker compose build benchmark
    echo ">> Rodando benchmark Multiprocessing com a amostra (datasample/)..."
    docker compose run --rm \
      -v "$ROOT/datasample:/datasample" \
      -e DATASET_PATH=/datasample/taxi_sample.csv \
      -e RESULTS_PATH=/results/metrics_sample.csv \
      benchmark
    ;;
  spark)
    echo ">> Build da imagem Spark..."
    docker compose build spark-benchmark
    echo ">> Rodando benchmark Spark com a amostra (datasample/)..."
    docker compose --profile spark run --rm -p 4040:4040 \
      -v "$ROOT/datasample:/datasample" \
      -e DATASET_PATH=/datasample/taxi_sample.csv \
      -e RESULTS_PATH=/results/metrics_spark_sample.csv \
      spark-benchmark
    ;;
  download)
    echo ">> Baixando dataset completo do NYC TLC (pode levar 20-60 min)..."
    docker compose --profile setup up downloader
    ;;
  full-1gb)
    docker compose build benchmark
    docker compose run --rm \
      -e DATASET_PATH=/datasets/taxi_1gb.csv \
      -e RESULTS_PATH=/results/metrics_1gb.csv \
      benchmark
    ;;
  *)
    echo "Ação desconhecida: $ACTION"
    echo "Use: sample | spark | download | full-1gb"
    exit 1
    ;;
esac
