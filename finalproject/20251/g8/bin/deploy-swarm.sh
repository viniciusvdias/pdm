#!/bin/bash
# O comando 'set -e' faz o script parar imediatamente se algum comando falhar.
set -e

# --- Variáveis de Configuração ---
IMAGE_NAME="jupyter-spark:1.0"
STACK_NAME="spark_cluster"
STACK_FILE="../docker-stack.yml" # Nome do seu arquivo de stack para o Swarm

echo "➡️  Iniciando a implantação no Docker Swarm..."

# Passo 1: Construir a imagem customizada do JupyterLab
echo "   [1/3] Construindo imagem '${IMAGE_NAME}'..."
docker build -t ${IMAGE_NAME} ../.

# Passo 2: Verificar e inicializar o Docker Swarm, se necessário
echo "   [2/3] Verificando status do Docker Swarm..."
if [ "$(docker info --format '{{.Swarm.LocalNodeState}}')" != "active" ]; then
  echo "       - Swarm não está ativo. Inicializando..."
  docker swarm init
else
  echo "       - Swarm já está ativo."
fi

# Passo 3: Implantar a stack de serviços
echo "   [3/3] Implantando a stack '${STACK_NAME}'..."
docker stack deploy -c ${STACK_FILE} ${STACK_NAME}

echo ""
echo "✅  Stack '${STACK_NAME}' implantada com sucesso!"
echo "   - Use 'docker stack services ${STACK_NAME}' para verificar o status."
echo "   - Spark Master UI: http://localhost:8080"
echo "   - JupyterLab: http://localhost:8888"