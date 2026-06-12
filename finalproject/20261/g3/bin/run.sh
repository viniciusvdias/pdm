#!/usr/bin/env bash
# Quick start: sobe a stack completa e processa o datasample.
# Requisito único: Docker + Docker Compose.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { cp .env.example .env; echo "[run] .env criado a partir de .env.example"; }

echo "[run] subindo a stack (build na primeira vez pode demorar)..."
docker compose up --build -d

echo "[run] aguardando submissão dos jobs Flink..."
# job-submitter sai com 0 após submeter settlement + aml.
docker compose wait job-submitter >/dev/null 2>&1 || true

cat <<EOF

================ Pix Settlement Engine no ar ================
 Flink Web UI : http://localhost:8081
 Grafana      : http://localhost:3000   (anon habilitado / admin:admin)
 Prometheus   : http://localhost:9090
 MinIO console: http://localhost:9001   (minioadmin:minioadmin)
=============================================================

Acompanhe a liquidação:
  docker compose logs -f producer consumer

Verifique a reconciliação (após o producer terminar):
  ./bin/reconcile.sh

Demonstração de falha (mate um TaskManager e veja o recovery):
  ./bin/kill_taskmanager.sh
EOF
