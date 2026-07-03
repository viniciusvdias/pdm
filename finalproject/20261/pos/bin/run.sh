#!/usr/bin/env bash
# Quick-start entry point: builds the image and runs the benchmark on the
# bundled datasample (or on DATA_DIR if you export it). Requires only Docker.
#
#   ./bin/run.sh                        # datasample, defaults
#   ORIGINS=16 ./bin/run.sh             # override number of sources
#   DATA_DIR=./data/rmsp_graph ORIGINS=64 ./bin/run.sh   # full graph
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

mkdir -p output

echo "==> Building and running the SSSP benchmark (Docker Compose)"
echo "    DATA_DIR=${DATA_DIR:-./datasample}  ORIGINS=${ORIGINS:-8}  REPETITIONS=${REPETITIONS:-3}  WORKERS=${WORKERS:-1,2,4}"
docker compose up --build --abort-on-container-exit --exit-code-from experiment

echo ""
echo "==> Done. Results are in ./output"
echo "    - output/metrics.csv               raw per-repetition measurements"
echo "    - output/plots/summary_table.csv   mean / std / speedup / efficiency"
echo "    - output/plots/*.png               charts"
