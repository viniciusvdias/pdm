#!/usr/bin/env bash
set -euo pipefail

JM=${JOBMANAGER_HOST:-flink-jobmanager}
JM_PORT=${JOBMANAGER_PORT:-8081}

echo "[submit] aguardando JobManager em ${JM}:${JM_PORT} ..."
until curl -sf "http://${JM}:${JM_PORT}/overview" >/dev/null 2>&1; do
  sleep 2
done
echo "[submit] JobManager disponível."

COMMON_ARGS=(
  -m "${JM}:${JM_PORT}"
  -pyclientexec python3
  -py
)

echo "[submit] submetendo settlement_job ..."
flink run -d "${COMMON_ARGS[@]}" /app/src/flink/settlement_job.py

echo "[submit] submetendo aml_job ..."
flink run -d "${COMMON_ARGS[@]}" /app/src/flink/aml_job.py

echo "[submit] jobs submetidos. Lista atual:"
flink list -m "${JM}:${JM_PORT}" || true
