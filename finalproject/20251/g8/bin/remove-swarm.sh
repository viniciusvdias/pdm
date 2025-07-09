#!/bin/bash
set -e

# --- Variável de Configuração ---
STACK_NAME="spark_cluster"

echo "➡️  Removendo a stack '${STACK_NAME}' do Docker Swarm..."

docker stack rm ${STACK_NAME}

echo "✅  Stack '${STACK_NAME}' removida com sucesso."