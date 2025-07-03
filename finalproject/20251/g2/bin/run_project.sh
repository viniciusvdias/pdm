#!/bin/bash

# Script principal para executar o projeto RU-UFLA Analytics
# Suporte para execução local, Docker Compose e Docker Swarm
# Autor: Grupo 2

set -e

echo "🚀 Iniciando projeto RU-UFLA Analytics..."

# Verificar se Docker está instalado
if ! command -v docker &> /dev/null; then
    echo "❌ Docker não encontrado. Por favor, instale o Docker primeiro."
    exit 1
fi

# Detectar qual versão do Docker Compose está disponível
DOCKER_COMPOSE_CMD=""
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    echo "❌ Docker Compose não encontrado. Por favor, instale o Docker Compose primeiro."
    echo "   Suporte para: 'docker-compose' (V1) ou 'docker compose' (V2)"
    exit 1
fi

echo "🔧 Usando: $DOCKER_COMPOSE_CMD"

# Configurar variáveis de ambiente para logging
export LOG_LEVEL=${LOG_LEVEL:-INFO}
export LOG_TO_FILE=${LOG_TO_FILE:-true}
export LOG_COLORS=${LOG_COLORS:-true}

# Função para mostrar ajuda
show_help() {
    echo "📖 Uso: $0 [COMANDO]"
    echo ""
    echo "Comandos disponíveis:"
    echo "  build           - Construir as imagens Docker"
    echo "  up              - Subir o cluster Spark local (padrão)"
    echo "  down            - Parar o cluster Spark local"
    echo "  restart         - Reiniciar o cluster Spark local"
    echo "  logs            - Mostrar logs dos containers"
    echo "  status          - Mostrar status dos containers"
    echo "  clean           - Limpar containers e volumes"
    echo "  test            - Executar testes de integração"
    echo "  analyze [mode]  - Executar análise de dados (mode: sample|complete)"
    echo "  info            - Mostrar informações do sistema"
    echo ""
    echo "🐳 Comandos Docker Swarm:"
    echo "  swarm-init      - Inicializar Docker Swarm"
    echo "  swarm-deploy    - Deploy no Docker Swarm"
    echo "  swarm-scale     - Escalar serviços no Swarm"
    echo "  swarm-remove    - Remover stack do Swarm"
    echo "  swarm-logs      - Logs dos serviços no Swarm"
    echo "  swarm-status    - Status dos serviços no Swarm"
    echo ""
    echo "  help            - Mostrar esta ajuda"
    echo ""
    echo "🌐 URLs importantes:"
    echo "  Spark Master UI:  http://localhost:8080"
    echo "  Spark App UI:     http://localhost:4040"
    echo "  Worker 1 UI:      http://localhost:8081"
    echo "  Worker 2 UI:      http://localhost:8082"
    echo ""
    echo "🔧 Variáveis de ambiente para logging:"
    echo "  LOG_LEVEL=DEBUG|INFO|WARNING|ERROR (padrão: INFO)"
    echo "  LOG_TO_FILE=true|false (padrão: true)"
    echo "  LOG_COLORS=true|false (padrão: true)"
    echo ""
    echo "💡 Exemplos de uso:"
    echo "  LOG_LEVEL=DEBUG $0 up"
    echo "  $0 analyze sample      # Análise com dados de amostra"
    echo "  $0 analyze complete    # Análise completa (padrão)"
    echo "  $0 analyze-local sample    # Análise local com amostra"
    echo "  $0 analyze-local complete  # Análise local completa"
}

# Função para construir imagens
build_images() {
    echo "🔨 Construindo imagens Docker..."
    $DOCKER_COMPOSE_CMD build
    echo "✅ Imagens construídas com sucesso!"
}

# Função para subir o cluster local
start_cluster() {
    echo "🚀 Iniciando cluster Spark local..."
    $DOCKER_COMPOSE_CMD up -d spark-master spark-worker-1 spark-worker-2
    
    echo "⏳ Aguardando inicialização dos serviços..."
    sleep 15
    
    echo "✅ Cluster iniciado com sucesso!"
    echo ""
    echo "🌐 Serviços disponíveis:"
    echo "  🔥 Spark Master UI:  http://localhost:8080"
    echo "  📈 Spark App UI:     http://localhost:4040 (quando app estiver rodando)"
    echo "  👷 Worker 1 UI:      http://localhost:8081"
    echo "  👷 Worker 2 UI:      http://localhost:8082"
    echo ""
    echo "📋 Comandos úteis:"
    echo "  Logs:       $0 logs"
    echo "  Análise:    $0 analyze [sample|complete]"
    echo "  Local:      $0 analyze-local [sample|complete]"
    echo "  Parar:      $0 down"
}

# Função para parar o cluster
stop_cluster() {
    echo "🛑 Parando cluster Spark..."
    $DOCKER_COMPOSE_CMD down
    echo "✅ Cluster parado com sucesso!"
}

# Função para reiniciar o cluster
restart_cluster() {
    echo "🔄 Reiniciando cluster Spark..."
    $DOCKER_COMPOSE_CMD restart
    echo "✅ Cluster reiniciado com sucesso!"
}

# Função para mostrar logs
show_logs() {
    echo "📋 Mostrando logs dos containers..."
    $DOCKER_COMPOSE_CMD logs -f
}

# Função para mostrar status
show_status() {
    echo "📊 Status dos containers:"
    $DOCKER_COMPOSE_CMD ps
}

# Função para limpeza
clean_all() {
    echo "🧹 Limpando recursos do projeto RU-UFLA Analytics..."
    
    # Parar e remover containers do projeto (sem remover volumes)
    $DOCKER_COMPOSE_CMD down --remove-orphans
    
    # Remover imagem específica do projeto se existir
    if docker images | grep -q "g2-spark-master\|g2-analytics\|ru-ufla-analytics"; then
        echo "🗑️  Removendo imagens do projeto..."
        docker images --format "table {{.Repository}}:{{.Tag}}" | grep -E "g2-|ru-ufla-analytics" | xargs -r docker rmi -f
    fi
    
    # Remover rede específica do projeto se não estiver sendo usada
    if docker network ls | grep -q "g2_spark-network"; then
        echo "🌐 Removendo rede do projeto..."
        docker network rm g2_spark-network 2>/dev/null || true
    fi
    
    echo "✅ Limpeza do projeto concluída!"
    echo "ℹ️  Recursos de outros projetos Docker foram preservados."
}

# Função para executar análise
run_analysis() {
    local mode=${1:-complete}
    
    case "$mode" in
        sample)
            echo "📊 Executando análise de dados do RU-UFLA (AMOSTRA)..."
            ;;
        complete)
            echo "📊 Executando análise de dados do RU-UFLA (COMPLETA)..."
            ;;
        *)
            echo "❌ Modo inválido: $mode. Use 'sample' ou 'complete'"
            return 1
            ;;
    esac
    
    # Verificar se o cluster está rodando
    if ! $DOCKER_COMPOSE_CMD ps | grep -q "Up"; then
        echo "⚠️  Cluster não está rodando. Iniciando..."
        start_cluster
        sleep 20
    fi
    
    # Executar análise com o modo especificado
    $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main analyze --master-url spark://spark-master:7077 --mode $mode
    echo "✅ Análise concluída!"
}

# Função para executar testes
run_tests() {
    echo "🧪 Executando testes de integração..."
    
    # Verificar se o cluster está rodando
    if ! $DOCKER_COMPOSE_CMD ps | grep -q "Up"; then
        echo "⚠️  Cluster não está rodando. Iniciando..."
        start_cluster
        sleep 20
    fi
    
    # Executar teste de conectividade
    $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main test-spark --master-url spark://spark-master:7077
    echo "✅ Testes concluídos com sucesso!"
}

# Função para mostrar informações
show_info() {
    echo "ℹ️  Mostrando informações do sistema..."
    
    if [ -d ".venv" ]; then
        .venv/bin/python -m src.main info
    else
        echo "⚠️  Ambiente virtual não encontrado. Execute 'analyze-local' primeiro."
    fi
}

# === FUNÇÕES DOCKER SWARM ===

# Função para inicializar Docker Swarm
init_swarm() {
    echo "🐳 Inicializando Docker Swarm..."
    
    if docker info | grep -q "Swarm: active"; then
        echo "ℹ️  Docker Swarm já está ativo"
    else
        docker swarm init
        echo "✅ Docker Swarm inicializado!"
    fi
}

# Função para deploy no Docker Swarm
deploy_swarm() {
    echo "🚀 Fazendo deploy no Docker Swarm..."
    
    # Verificar se Swarm está ativo
    if ! docker info | grep -q "Swarm: active"; then
        echo "⚠️  Docker Swarm não está ativo. Inicializando..."
        init_swarm
    fi
    
    # Construir imagem
    docker build -t ru-ufla-analytics:latest .
    
    # Deploy da stack
    docker stack deploy -c docker-compose.swarm.yml ru-analytics
    
    echo "✅ Deploy no Swarm concluído!"
    echo "🌐 Acesse http://localhost:8080 para o Spark Master UI"
    
    # Mostrar status
    swarm_status
}

# Função para escalar serviços no Swarm
scale_swarm() {
    local workers=${1:-3}
    echo "📈 Escalando workers para $workers réplicas..."
    
    docker service scale ru-analytics_spark-worker=$workers
    echo "✅ Scaling concluído!"
}

# Função para remover stack do Swarm
remove_swarm() {
    echo "🗑️  Removendo stack do Docker Swarm..."
    docker stack rm ru-analytics
    echo "✅ Stack removida!"
}

# Função para logs do Swarm
swarm_logs() {
    echo "📋 Logs dos serviços no Swarm:"
    docker service logs -f ru-analytics_spark-master
}

# Função para status do Swarm
swarm_status() {
    echo "📊 Status dos serviços no Docker Swarm:"
    docker stack services ru-analytics
    echo ""
    echo "📝 Tarefas (containers) por serviço:"
    docker stack ps ru-analytics
}

# Processar comando
case "${1:-up}" in
    build)
        build_images
        ;;
    up|start)
        start_cluster
        ;;
    down|stop)
        stop_cluster
        ;;
    restart)
        restart_cluster
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    clean)
        clean_all
        ;;
    test)
        run_tests
        ;;
    analyze)
        run_analysis $2
        ;;
    info)
        show_info
        ;;
    # Comandos Docker Swarm
    swarm-init)
        init_swarm
        ;;
    swarm-deploy)
        deploy_swarm
        ;;
    swarm-scale)
        scale_swarm $2
        ;;
    swarm-remove)
        remove_swarm
        ;;
    swarm-logs)
        swarm_logs
        ;;
    swarm-status)
        swarm_status
        ;;
    help)
        show_help
        ;;
    *)
        echo "❌ Comando desconhecido: $1"
        echo ""
        show_help
        exit 1
        ;;
esac 