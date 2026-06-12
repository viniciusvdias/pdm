#!/usr/bin/env bash
# Demonstração de falha: mata um TaskManager. O Flink recupera do último
# checkpoint; em EXACTLY_ONCE o saldo final permanece correto (rode reconcile.sh).
# Uso: ./bin/kill_taskmanager.sh   (mata um TM imediatamente)
set -euo pipefail
cd "$(dirname "$0")/.."

IDS="$(docker compose ps -q flink-taskmanager)"
if [ -z "$IDS" ]; then
  echo "[kill] nenhum TaskManager em execução."; exit 1
fi
TARGET="$(echo "$IDS" | head -1)"
echo "[kill] matando TaskManager $TARGET ..."
docker kill "$TARGET"
echo "[kill] morto. O Flink reagendará o job e recuperará do último checkpoint."
echo "[kill] dica: garanta TaskManagers suficientes:"
echo "       docker compose up -d --scale flink-taskmanager=3"
