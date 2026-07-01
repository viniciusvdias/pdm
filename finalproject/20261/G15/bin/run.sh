#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# SMOKE TEST (out of the box) - roda o pipeline completo sobre a amostra do datasample/.
# Prova que o projeto executa so com Docker. Os NUMEROS aqui sao INCONCLUSIVOS de proposito:
# com ~40 imagens o dataset cabe na RAM/cache e o Small File Problem nao aparece.
# Para reproduzir os resultados do relatorio, use bin/run_full.sh (dataset completo).
#
# Uso:  ./bin/run.sh
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # raiz do grupo (gX/)
cd "$ROOT"
mkdir -p output
COMPOSE="docker compose -f misc/docker-compose.yml"

echo ">>> [1/2] build da imagem (pode demorar na 1a vez: imagem NGC da NVIDIA)"
$COMPOSE build

echo ">>> [2/2] pipeline sobre o datasample (02 baseline -> 03 ETL -> 04 otimizado)"
$COMPOSE run --rm \
  -e DATA_DIR=/tf/output \
  -e RAW_DIR=/tf/datasample/sample_images \
  -e PARQUET_DIR=/tf/output/optimized \
  -e RESULTS_DIR=/tf/output/results \
  -e N_ROUNDS=2 -e BATCH_SIZE=16 -e N_PASSES=3 -e IMGS_PER_FILE=20 \
  bigdata python run_all.py

echo ""
echo ">>> Concluido. Resultados (CSVs + PNGs) em: ./output/results/"
ls -la output/results/ 2>/dev/null || true
