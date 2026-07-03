#!/usr/bin/env bash
# =========================================================================== #
# Quick start — pipeline completo com a amostra em datasample/, so com Docker.
#
#   bin/run.sh                       # roda com datasample/ (amostra ~566KB)
#   DATA_DIR_HOST=../data bin/run.sh # roda com o dataset completo (ver README)
#
# Etapas: sobe infra (Kafka + Spark) -> ingere a amostra (container producer)
# -> submete o job de Structured Streaming -> escreve a saida em output/.
# Nao exige Python nem venv no host: tudo roda em containers.
# =========================================================================== #
set -euo pipefail

# Raiz do projeto (g11/) = pai deste script; compose vive em misc/.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose -f ${ROOT}/misc/docker-compose.yml"

# Flags do producer (default: --interleave para amostrar os 4 service_types).
PRODUCER_ARGS=("${@:---interleave}")

# A imagem bitnami/spark roda como UID 1001. Criamos output/ e checkpoints/ com
# permissao aberta ANTES do bind-mount, senao o Docker os cria como root e o
# Spark nao consegue escrever (cache ivy, resultados, state store).
echo "==> [0/4] Preparando diretorios de saida (output/, checkpoints/)..."
mkdir -p "${ROOT}/output" "${ROOT}/checkpoints"
chmod 777 "${ROOT}/output" "${ROOT}/checkpoints" 2>/dev/null || true

echo "==> [1/4] Subindo infraestrutura (Kafka + Spark master + 1 worker)..."
${COMPOSE} up -d

echo "==> [2/4] Aguardando o Kafka ficar healthy..."
until [ "$(${COMPOSE} ps --format '{{.Health}}' kafka 2>/dev/null)" = "healthy" ]; do
  sleep 3
  echo "    ...ainda subindo"
done
echo "    Kafka pronto."

echo "==> [3/4] Ingerindo a amostra (container producer)..."
${COMPOSE} --profile ingest run --rm producer "${PRODUCER_ARGS[@]}"

echo "==> [4/4] Submetendo o job de Structured Streaming..."
${COMPOSE} exec -T spark-master /app/spark-submit.sh --idle-stop 3

echo
echo "==> CONCLUIDO. Saida em:"
echo "      ${ROOT}/output/daily_peaks/"
echo "      ${ROOT}/output/avg_duration_by_zone/"
echo "    Para derrubar tudo:  docker compose -f misc/docker-compose.yml down -v"
