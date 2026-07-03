#!/usr/bin/env bash
# Single-VM pipeline: download (if needed) → benchmark Ray+Dask → report.
set -euo pipefail
cd /app

: "${GRAPH_RAW_PATH:=data/raw/soc-orkut-relationships.txt}"
: "${BENCHMARK_FRACTIONS:=100}"
: "${BENCHMARK_RUNS:=3}"
: "${BENCHMARK_STAMP:=}"
: "${BENCHMARK_SEEDS:=}"
: "${BENCHMARK_WORKERS:=}"

if [[ -z "$BENCHMARK_STAMP" ]]; then
  BENCHMARK_STAMP=$(date -u +%Y%m%dT%H%M%S)
fi
export BENCHMARK_STAMP

benchmark_backend_flag() {
  case "$1" in
    ray) echo "--ray-only" ;;
    dask) echo "--dask-only" ;;
    both) echo "" ;;
    *)
      echo "Unknown BENCHMARK_BACKEND: $1" >&2
      exit 1
      ;;
  esac
}

run_benchmark() {
  if [[ ! -f "$GRAPH_RAW_PATH" ]]; then
    bash bin/download_dataset.sh
  fi
  local backend=$1
  local append_flag=()
  [[ "$2" == "1" ]] && append_flag=(--append)
  local stamp_args=()
  [[ -n "$BENCHMARK_STAMP" ]] && stamp_args=(--run-stamp "$BENCHMARK_STAMP")
  local backend_flag
  backend_flag=$(benchmark_backend_flag "$backend")
  local workers_flag=()
  [[ -n "$BENCHMARK_WORKERS" ]] && workers_flag=(--workers "$BENCHMARK_WORKERS")
  python -m cli.main benchmark \
    --input "$GRAPH_RAW_PATH" \
    --fractions "$BENCHMARK_FRACTIONS" \
    --runs "$BENCHMARK_RUNS" \
    "${stamp_args[@]}" \
    ${backend_flag:+$backend_flag} \
    "${workers_flag[@]}" \
    "${append_flag[@]}"
}

run_report() {
  local stamp_args=()
  if [[ -n "$BENCHMARK_STAMP" ]]; then
    stamp_args=(--input-csv "reports/metrics_raw_${BENCHMARK_STAMP}.csv")
  fi
  python -m cli.main report "${stamp_args[@]}"
}

: "${BENCHMARK_BACKEND:=both}"

if [[ ! -f "$GRAPH_RAW_PATH" ]]; then
  bash bin/download_dataset.sh
fi

case "$BENCHMARK_BACKEND" in
  both)
    run_benchmark ray 0
    run_benchmark dask 1
    ;;
  ray|dask)
    run_benchmark "$BENCHMARK_BACKEND" 0
    ;;
  *)
    echo "BENCHMARK_BACKEND must be both, ray, or dask" >&2
    exit 1
    ;;
esac
run_report
