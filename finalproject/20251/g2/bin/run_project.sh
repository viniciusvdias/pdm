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
    echo "  analyze [mode] [periods]  - Executar análise de dados (mode: sample|complete)"
    echo "  experiments [mode] [iterations] [periods] - Executar experimentos isolados (mode: sample|complete, iterations: número >= 1)"
    echo "  consolidate [pattern] - Consolidar resultados de experimentos"
    echo ""
    echo "🐳 Comandos Docker Swarm:"
    echo "  swarm-init      - Inicializar Docker Swarm"
    echo "  swarm-deploy    - Deploy no Docker Swarm (4 nós)"
    echo "  swarm-scale     - Escalar serviços no Swarm"
    echo "  swarm-remove    - Remover stack do Swarm"
    echo "  swarm-logs      - Logs dos serviços no Swarm"
    echo "  swarm-status    - Status dos serviços no Swarm"
    echo "  swarm-analyze   - Executar análise no cluster Swarm"
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
    echo "  $0 analyze sample                      # Análise com dados de amostra"
    echo "  $0 analyze complete                    # Análise completa (padrão)"
    echo "  $0 analyze sample 2024/1,2024/2          # Análise do ano 2024 com dados de amostra"
    echo "  $0 analyze complete 2024/1,2024/2        # Análise do ano 2024 com dados completos"
    echo "  $0 experiments sample 5                # Experimentos isolados com 5 iterações (dados de amostra)"
    echo "  $0 experiments complete 3 2024/1,2024/2 # Experimentos completos com 3 iterações (ano 2024)"
    echo "  $0 consolidate experiment_complete_*   # Consolidar resultados de experimentos completos"
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

# Função para executar análise
run_analysis() {
    local mode=${1:-complete}
    local periods=$2
 
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
    if [ -z "$periods" ]; then
        $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main analyze --master-url spark://spark-master:7077 --mode $mode
    else
        $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main analyze --master-url spark://spark-master:7077 --mode $mode --periods $periods
    fi
    echo "✅ Análise concluída!"
}

# Função para executar experimentos isolados
run_experiments() {
    local mode=${1:-complete}
    local iterations=${2:-3}
    local periods=$3
 
    case "$mode" in
        sample)
            echo "🧪 Executando experimentos isolados do RU-UFLA (AMOSTRA)..."
            ;;
        complete)
            echo "🧪 Executando experimentos isolados do RU-UFLA (COMPLETA)..."
            ;;
        *)
            echo "❌ Modo inválido: $mode. Use 'sample' ou 'complete'"
            return 1
            ;;
    esac
    
    # Validar número de iterações
    if ! [[ "$iterations" =~ ^[0-9]+$ ]] || [ "$iterations" -lt 1 ]; then
        echo "❌ Número de iterações inválido: $iterations. Use um número inteiro >= 1"
        return 1
    fi
    
    echo "🔄 Executando $iterations iterações por configuração"
    
    # Verificar se o cluster está rodando
    if ! $DOCKER_COMPOSE_CMD ps | grep -q "Up"; then
        echo "⚠️  Cluster não está rodando. Iniciando..."
        start_cluster
        sleep 20
    fi
    
    # Executar experimentos isolados com os parâmetros especificados
    if [ -z "$periods" ]; then
        $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main experiments --master-url spark://spark-master:7077 --mode $mode --iterations $iterations
    else
        $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main experiments --master-url spark://spark-master:7077 --mode $mode --iterations $iterations --periods $periods
    fi
    echo "✅ Experimentos isolados concluídos!"
}

# Função para consolidar resultados de experimentos
consolidate_results() {
    local pattern=${1:-"experiment_*"}
    
    echo "📊 Consolidando resultados de experimentos..."
    echo "   Padrão de busca: $pattern"
    
    # Verificar se o cluster está rodando
    if ! $DOCKER_COMPOSE_CMD ps | grep -q "Up"; then
        echo "⚠️  Cluster não está rodando. Iniciando..."
        start_cluster
        sleep 20
    fi
    
    $DOCKER_COMPOSE_CMD run --rm analytics /app/.venv/bin/python -m src.main consolidate --pattern "$pattern"
    echo "✅ Consolidação concluída!"
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
    local workers=${1:-8}
    echo "📈 Escalando workers para $workers réplicas..."
    
    if ! docker stack services ru-analytics | grep -q "ru-analytics_spark-worker"; then
        echo "❌ Stack ru-analytics não encontrada. Execute 'swarm-deploy' primeiro."
        return 1
    fi
    
    docker service scale ru-analytics_spark-worker=$workers
    echo "✅ Scaling concluído!"
    
    # Mostrar status atualizado
    echo "📊 Status atualizado:"
    docker service ls --filter name=ru-analytics
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
    docker stack ps ru-analytics --no-trunc
    echo ""
    echo "🔍 Nós do cluster:"
    docker node ls
}

# Função para executar análise no Swarm
run_swarm_analysis() {
    local mode=${1:-complete}
    local periods=$2
    
    case "$mode" in
        sample)
            echo "📊 Executando análise de dados do RU-UFLA (AMOSTRA) no cluster Swarm..."
            ;;
        complete)
            echo "📊 Executando análise de dados do RU-UFLA (COMPLETA) no cluster Swarm..."
            ;;
        *)
            echo "❌ Modo inválido: $mode. Use 'sample' ou 'complete'"
            return 1
            ;;
    esac
    
    # Verificar se o cluster Swarm está rodando
    if ! docker stack services ru-analytics | grep -q "ru-analytics_spark-master"; then
        echo "⚠️  Cluster Swarm não está rodando. Fazendo deploy..."
        deploy_swarm
        sleep 60
    fi
    
    # Executar análise com o modo especificado
    if [ -z "$periods" ]; then
        docker service create --name ru-analytics-job-$(date +%s) \
            --network ru-analytics_spark-network \
            --mount type=bind,source=$(pwd)/misc,destination=/app/misc \
            --mount type=bind,source=$(pwd)/datasample,destination=/app/datasample \
            --env SPARK_MASTER_URL=spark://spark-master:7077 \
            --env DOCKER_SWARM_MODE=true \
            --constraint 'node.role == manager' \
            --restart-condition none \
            ru-ufla-analytics:latest \
            /app/.venv/bin/python -m src.main analyze --master-url spark://spark-master:7077 --mode $mode
    else
        docker service create --name ru-analytics-job-$(date +%s) \
            --network ru-analytics_spark-network \
            --mount type=bind,source=$(pwd)/misc,destination=/app/misc \
            --mount type=bind,source=$(pwd)/datasample,destination=/app/datasample \
            --env SPARK_MASTER_URL=spark://spark-master:7077 \
            --env DOCKER_SWARM_MODE=true \
            --constraint 'node.role == manager' \
            --restart-condition none \
            ru-ufla-analytics:latest \
            /app/.venv/bin/python -m src.main analyze --master-url spark://spark-master:7077 --mode $mode --periods $periods
    fi
    
    echo "✅ Job de análise iniciado no cluster Swarm!"
    echo "💡 Use 'docker service logs -f <job-name>' para acompanhar o progresso"
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
    analyze)
        run_analysis $2 $3
        ;;
    experiments)
        run_experiments $2 $3 $4
        ;;
    consolidate)
        consolidate_results $2
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
    swarm-analyze)
        run_swarm_analysis $2 $3
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