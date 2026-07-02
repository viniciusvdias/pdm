#!/usr/bin/env bash
# Runs the project with all CSV files present in the sample data directory.
# Usage: ./bin/run.sh [N_WORKERS]
set -e
N_WORKERS=${1:-1}
WORKLOADS=${WORKLOADS:-1,2,3}
RUNS=${RUNS:-1}
REPEAT_COUNT=${REPEAT_COUNT:-100}
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA_PATH="${DATA_PATH:-$PROJECT_ROOT/datasample}"

cd "$PROJECT_ROOT/misc"

echo "[INFO] Starting Spark job with workers=$N_WORKERS runs=$RUNS workloads=$WORKLOADS repeat_count=$REPEAT_COUNT data_dir=$DATA_PATH"
N_WORKERS=$N_WORKERS RUNS=$RUNS WORKLOADS=$WORKLOADS REPEAT_COUNT=$REPEAT_COUNT DATA_PATH="$DATA_PATH" \
  docker compose up --scale spark-worker="$N_WORKERS" --abort-on-container-exit --exit-code-from spark-job
docker compose down -v
