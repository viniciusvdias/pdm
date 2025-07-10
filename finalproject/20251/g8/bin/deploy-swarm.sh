#!/bin/bash
# O comando 'set -e' faz o script parar imediatamente se algum comando falhar.
set -e

# --- Variáveis de Configuração ---
IMAGE_NAME="jupyter-spark:1.0"
STACK_NAME="spark_cluster"
STACK_FILE="../docker-stack.yml" # Nome do seu arquivo de stack para o Swarm

# --- Configuração de Workers ---
# Define a quantidade de workers. Usa o primeiro argumento do script ($1).
# Se nenhum argumento for passado, o padrão será 3.
# O 'export' é crucial para que o 'docker stack deploy' possa ler a variável.
export WORKER_COUNT=${1:-2}

echo "➡️  Iniciando a implantação no Docker Swarm..."
echo "    - Quantidade de workers a serem criados: ${WORKER_COUNT}"

# Passo 1: Construir a imagem customizada do JupyterLab
echo "   [1/3] Construindo imagem '${IMAGE_NAME}'..."
docker build -t ${IMAGE_NAME} ../.

# Passo 2: Verificar e inicializar o Docker Swarm, se necessário
echo "   [2/3] Verificando status do Docker Swarm..."
if [ "$(docker info --format '{{.Swarm.LocalNodeState}}')" != "active" ]; then
  echo "       - Swarm não está ativo. Inicializando..."
  # Se você encontrar um erro sobre múltiplos IPs, use a linha abaixo com seu IP:
  # docker swarm init --advertise-addr <SEU_IP_PRINCIPAL>
  docker swarm init
else
  echo "       - Swarm já está ativo."
fi

# Passo 3: Implantar a stack de serviços
echo "   [3/3] Implantando a stack '${STACK_NAME}'..."
# O comando 'deploy' usará a variável WORKER_COUNT que foi exportada
docker stack deploy -c ${STACK_FILE} ${STACK_NAME}

echo ""
echo "✅  Stack '${STACK_NAME}' implantada com sucesso com ${WORKER_COUNT} workers!"
echo "   - Use 'docker stack services ${STACK_NAME}' para verificar o status."
echo "   - Spark Master UI: http://localhost:8080"
echo "   - JupyterLab: http://localhost:8888"