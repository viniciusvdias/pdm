#!/usr/bin/env bash
# Reconciliação financeira: baseline batch × saldo final do stream (Postgres).
# Requer a stack no ar (./bin/run.sh) e o producer já tendo drenado o input.
set -euo pipefail
 
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "$0")/.."

# Lê INPUT_CSV do .env (default: amostra).
INPUT_CSV="$(grep -E '^INPUT_CSV=' .env 2>/dev/null | head -1 | cut -d= -f2-)"
INPUT_CSV="${INPUT_CSV:-./datasample/sample.csv.gz}"
ABS_IN="$(cd "$(dirname "$INPUT_CSV")" && pwd)/$(basename "$INPUT_CSV")"

mkdir -p results

echo "[reconcile] computando baseline batch determinístico..."
docker compose run --rm --no-deps \
  -v "$PWD/results":/results \
  -v "$ABS_IN":/data/input.csv:ro \
  consumer python /app/src/batch/baseline.py \
    --input /data/input.csv --output /results/baseline.csv

echo "[reconcile] comparando com o saldo final do stream..."
docker compose run --rm --no-deps \
  -v "$PWD/results":/results \
  consumer python /app/src/experiments/reconcile.py \
    --baseline /results/baseline.csv --report /results/diffs.csv
