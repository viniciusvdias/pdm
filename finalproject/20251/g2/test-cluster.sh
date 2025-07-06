#!/bin/bash

# Script para testar o funcionamento do cluster Apache Spark
# Autor: Grupo 2

set -e

echo "🧪 Testando funcionamento do cluster Apache Spark no Docker Swarm"
echo "=================================================================="

# Verificar se está em ambiente Swarm
if ! docker info | grep -q "Swarm: active"; then
    echo "❌ Docker Swarm não está ativo!"
    echo "Execute: docker swarm init"
    exit 1
fi

echo "✅ Docker Swarm está ativo"

# Verificar nós do cluster
echo ""
echo "🔍 Verificando nós do cluster:"
docker node ls

# Verificar se a imagem existe
if ! docker image inspect ru-ufla-analytics:latest > /dev/null 2>&1; then
    echo ""
    echo "⚠️  Imagem ru-ufla-analytics:latest não encontrada. Construindo..."
    docker build -t ru-ufla-analytics:latest .
fi

echo ""
echo "✅ Imagem ru-ufla-analytics:latest disponível"

# Verificar se a stack já está rodando
if docker stack ls | grep -q "ru-analytics"; then
    echo ""
    echo "⚠️  Stack ru-analytics já está rodando. Removendo para teste limpo..."
    docker stack rm ru-analytics
    echo "⏳ Aguardando remoção completa..."
    sleep 30
fi

# Fazer deploy da stack
echo ""
echo "🚀 Fazendo deploy da stack no cluster..."
docker stack deploy -c docker-compose.swarm.yml ru-analytics

echo ""
echo "⏳ Aguardando inicialização dos serviços..."
sleep 45

# Verificar serviços
echo ""
echo "📊 Status dos serviços:"
docker stack services ru-analytics

# Verificar se todos os serviços estão running
echo ""
echo "🔍 Verificando tasks dos serviços:"
docker stack ps ru-analytics --no-trunc

# Aguardar mais tempo para garantir que tudo esteja rodando
echo ""
echo "⏳ Aguardando estabilização do cluster..."
sleep 60

# Testar conectividade Spark
echo ""
echo "🧪 Testando conectividade com Spark Master..."

# Criar um serviço temporário para testar
docker service create --name test-spark-connectivity \
    --network ru-analytics_spark-network \
    --env SPARK_MASTER_URL=spark://spark-master:7077 \
    --env DOCKER_SWARM_MODE=true \
    --constraint 'node.role == manager' \
    --restart-condition none \
    ru-ufla-analytics:latest \
    /app/.venv/bin/python -m src.main test-spark --master-url spark://spark-master:7077

echo ""
echo "⏳ Aguardando teste de conectividade..."
sleep 30

# Verificar logs do teste
echo ""
echo "📋 Logs do teste de conectividade:"
docker service logs test-spark-connectivity

# Limpar serviço de teste
docker service rm test-spark-connectivity

# Mostrar resumo final
echo ""
echo "📈 Resumo do Cluster:"
echo "==================="
echo "🔥 Spark Master UI: http://localhost:8080"
echo "📊 Spark App UI: http://localhost:4040 (durante execução)"
echo "👷 Workers UIs: http://localhost:8081-8084"
echo ""
echo "📊 Recursos configurados:"
echo "- 8 Workers distribuídos nas 4 VMs"
echo "- 4 cores por worker (32 cores total)"
echo "- 12GB RAM por worker (96GB total)"
echo "- Driver: 2 cores, 8GB RAM"
echo ""
echo "🚀 Comandos úteis:"
echo "- Status: ./bin/run_project.sh swarm-status"
echo "- Análise: ./bin/run_project.sh swarm-analyze complete"
echo "- Logs: ./bin/run_project.sh swarm-logs"
echo "- Escalar: ./bin/run_project.sh swarm-scale 12"
echo "- Remover: ./bin/run_project.sh swarm-remove"
echo ""
echo "✅ Teste do cluster concluído!"
echo "💡 Acesse http://localhost:8080 para ver o Spark Master UI" 