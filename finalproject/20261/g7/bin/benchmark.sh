#!/usr/bin/env bash
# Benchmarking script — runs the job multiple times and extracts timing lines.
# Usage: ./bin/benchmark.sh [N_WORKERS] [RUNS] [DATA_PATH]
#   N_WORKERS  number of Spark workers      (default: 1)
#   RUNS       number of repetitions        (default: 5)
#   DATA_PATH  host path to data directory  (default: ../datasample)
#
# Example (full dataset, 4 workers, 5 runs):
#   DATA_PATH=/path/to/data ./bin/benchmark.sh 4 5
set -e
N_WORKERS=${1:-1}
RUNS=${2:-5}
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA_PATH=${DATA_PATH:-$PROJECT_ROOT/datasample}
WORKLOADS=${WORKLOADS:-1,2,3}
REPEAT_COUNT=${REPEAT_COUNT:-100}

cd "$PROJECT_ROOT/misc"

echo "=== Benchmark | Workers=$N_WORKERS | Runs=$RUNS | Workloads=$WORKLOADS | Repeat count=$REPEAT_COUNT | Data dir=$DATA_PATH ==="
for i in $(seq 1 "$RUNS"); do
    echo ""
    echo "--- Run $i/$RUNS ---"
    N_WORKERS=$N_WORKERS RUNS=1 WORKLOADS=$WORKLOADS REPEAT_COUNT=$REPEAT_COUNT DATA_PATH=$DATA_PATH \
        docker compose up --scale spark-worker="$N_WORKERS" --abort-on-container-exit --exit-code-from spark-job \
        2>&1 | grep -E "\[TIMING\]|\[SUMMARY\]|\[INFO\]"
    docker compose down -v
done
echo ""
echo "=== Done. Collect the [TIMING] lines above to fill the results table. ==="
