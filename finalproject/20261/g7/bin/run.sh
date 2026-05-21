#!/usr/bin/env bash
# Runs the project with sample data (datasample/esea_sample.csv).
# Usage: ./bin/run.sh [N_WORKERS]
set -e
N_WORKERS=${1:-1}
cd "$(dirname "$0")/../misc"
N_WORKERS=$N_WORKERS docker compose up --abort-on-container-exit --exit-code-from spark-job
docker compose down
