#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# DATASET COMPLETO - reproduz os resultados do relatorio.
# Pre-requisito: baixar o Dogs vs. Cats e colocar o train.zip; ver bin/download_data.sh.
#
# Faz: 01 preparo (move labels + DUPLICA ate TARGET_GB) -> 02 baseline -> 03 ETL -> 04 otimizado.
# A duplicacao estoura a RAM do conteiner e evita o OS cache -> forca o gargalo de I/O medido.
#
# Uso:  ./bin/run_full.sh
# Ajustes (opcionais):  TARGET_GB=8 N_ROUNDS=3 BATCH_SIZE=256 ./bin/run_full.sh
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p output/data/raw_jpg output/results
COMPOSE="docker compose -f misc/docker-compose.yml"

if [ -z "$(ls -A output/data/raw_jpg 2>/dev/null | grep -iE '\.jpg$|train\.zip$' || true)" ]; then
  echo "[!] Nenhuma imagem/zip em ./output/data/raw_jpg."
  echo "    Rode ./bin/download_data.sh (ou coloque o train.zip ali). Ver README secao 2."
  exit 1
fi

echo ">>> build da imagem"
$COMPOSE build

echo ">>> pipeline completo (01 preparo -> 02 -> 03 -> 04)"
$COMPOSE run --rm \
  -e DATA_DIR=/tf/output/data \
  -e RAW_DIR=/tf/output/data/raw_jpg \
  -e PARQUET_DIR=/tf/output/data/optimized \
  -e RESULTS_DIR=/tf/output/results \
  -e RUN_PREPARE=1 \
  -e TARGET_GB="${TARGET_GB:-1.5}" \
  -e N_ROUNDS="${N_ROUNDS:-3}" \
  -e BATCH_SIZE="${BATCH_SIZE:-256}" \
  bigdata python run_all.py

echo ""
echo ">>> Concluido. Resultados (CSVs + PNGs) em: ./output/results/"
ls -la output/results/ 2>/dev/null || true
