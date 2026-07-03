#!/usr/bin/env bash
# Quick benchmark on the bundled 0.1% sample (no Orkut download needed).
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose run --rm \
  -e GRAPH_RAW_PATH=datasample/orkut_0p1pct.npz \
  -e BENCHMARK_FRACTIONS=0.1 \
  -e BENCHMARK_RUNS=1 \
  lpa
