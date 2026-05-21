#!/usr/bin/env bash
# Benchmarking script — runs the job multiple times and extracts timing lines.
# Usage: ./bin/benchmark.sh [N_WORKERS] [RUNS] [DATA_PATH] [DATA_FILE]
#   N_WORKERS  number of Spark workers      (default: 1)
#   RUNS       number of repetitions        (default: 5)
#   DATA_PATH  host path to data directory  (default: ../datasample)
#   DATA_FILE  CSV filename inside DATA_PATH (default: esea_sample.csv)
#
# Example (full dataset, 4 workers, 5 runs):
#   DATA_PATH=/path/to/data DATA_FILE=esea_master_dmg_demos.part1.csv \
#     ./bin/benchmark.sh 4 5
set -e
N_WORKERS=${1:-1}
RUNS=${2:-5}
DATA_PATH=${DATA_PATH:-../datasample}
DATA_FILE=${DATA_FILE:-esea_sample.csv}

cd "$(dirname "$0")/../misc"

echo "=== Benchmark | Workers=$N_WORKERS | Runs=$RUNS | File=$DATA_FILE ==="
for i in $(seq 1 "$RUNS"); do
    echo ""
    echo "--- Run $i/$RUNS ---"
    N_WORKERS=$N_WORKERS DATA_PATH=$DATA_PATH DATA_FILE=$DATA_FILE \
        docker compose up --abort-on-container-exit --exit-code-from spark-job \
        2>&1 | grep -E "\[TIMING\]|\[SUMMARY\]|\[INFO\]"
    docker compose down -v
done
echo ""
echo "=== Done. Collect the [TIMING] lines above to fill the results table. ==="
