#!/bin/bash

PROJECT_NAME="g7"
STACK_NAME="g7stack"
NETWORK_NAME="g7net"
JUPYTER_NAME="jupytercli-g7"
COMPOSE_FILE="docker-compose.yml"
STACK_FILE="docker-stack.yml"
MODE=""   # compose | swarm

# Detecta o modo de operação (com base na rede g7net)
function detect_mode() {
  local driver
  driver=$(docker network inspect "$NETWORK_NAME" --format '{{.Driver}}' 2>/dev/null)

  if [ "$driver" == "overlay" ]; then
    MODE="swarm"
  elif [ "$driver" == "bridge" ]; then
    MODE="compose"
  else
    MODE="unknown"
  fi
}

# Swarm: cria rede overlay
function create_network_swarm() {
  echo ">> Criando rede Docker Swarm '$NETWORK_NAME'..."

  docker swarm init 2>/dev/null || true
  docker network create --scope=swarm --attachable -d overlay "$NETWORK_NAME" 2>/dev/null || echo "Rede '$NETWORK_NAME' já existe"
}

# Compose: cria rede bridge
function create_network_compose() {
  echo ">> Criando rede bridge '$NETWORK_NAME'..."

  docker network create --driver bridge "$NETWORK_NAME" 2>/dev/null || echo "Rede '$NETWORK_NAME' já existe"
}

# Build da imagem Jupyter
function build() {
  echo ">> Construindo imagem Docker '$JUPYTER_NAME'..."

  docker build -t $JUPYTER_NAME .
}

# Inicia container Jupyter
function start_jupyter() {
  echo ">> Iniciando container '$JUPYTER_NAME'..."

  local exists=$(docker ps -a --filter "name=^/${JUPYTER_NAME}$" --format "{{.Names}}")

  if [ "$exists" == "$JUPYTER_NAME" ]; then
    echo ">> Container já existe. Reiniciando..."
    docker start "$JUPYTER_NAME" > /dev/null
  else
    echo ">> Criando novo container '$JUPYTER_NAME'..."
    docker run -d \
      --name "$JUPYTER_NAME" \
      -p 8888:8888 \
      -p 4040:4040 \
      -p 8501:8501 \
      --network "$NETWORK_NAME" \
      "$JUPYTER_NAME" \
      jupyter lab --no-browser --ip=0.0.0.0 --allow-root > /dev/null
  fi

  # espera token ficar pronto
  sleep 5
}

# Para container Jupyter
function stop_jupyter() {
  echo ">> Parando container '$JUPYTER_NAME'..."

  docker stop "$JUPYTER_NAME" 2>/dev/null || true
}

# Remove container Jupyter
function remove_jupyter() {
  echo ">> Removendo container '$JUPYTER_NAME'..."

  docker rm "$JUPYTER_NAME" 2>/dev/null || true
}

# Swarm deploy
function deploy() {
  local replicas=$1

  if [ -z "$replicas" ]; then   # valor vazio
    if [ -f "$STACK_FILE" ]; then
      replicas=$(awk '
        $0 ~ /spark-worker-g7:/ { in_block=1 }
        in_block && $0 ~ /deploy:/ { in_deploy=1 }
        in_block && in_deploy && $0 ~ /replicas:/ {
          print $2
          exit
        }
      ' "$STACK_FILE")
    fi

    # fallback
    replicas=${replicas:-2}   # padrão: 2 workers
  fi
 
  export SPARK_WORKER_REPLICAS=$replicas

  create_network_swarm
  start_jupyter

  echo ">> Gerando '$STACK_FILE' com $SPARK_WORKER_REPLICAS worker(s)..."
  envsubst < $(pwd)/misc/docker-stack.template.yml > "$STACK_FILE"

  echo ">> Deploy do stack '$STACK_NAME'..."
  docker stack deploy --compose-file "$STACK_FILE" "$STACK_NAME"
  print_urls
}

# Compose up
function up() {
  create_network_compose
  start_jupyter

  echo ">> Subindo containers com docker-compose..."

  docker compose -f "$COMPOSE_FILE" up -d
  print_urls
}

# "Para" todos os containers
function stop_all() {
  echo ">> Parando containers..."
  
  stop_jupyter

  if [ "$MODE" = "swarm" ]; then
    docker stack rm "$STACK_NAME"
  elif [ "$MODE" == "compose" ]; then
    docker compose -f "$COMPOSE_FILE" down
  else
    echo ">> Modo de operação desconhecido. Rede $NETWORK_NAME não detectada"
  fi
}

# Reinicia o modo de operação
function restart() {
  stop_all
  if [ "$MODE" = "swarm" ]; then
    deploy
  elif [ "$MODE" == "compose" ]; then
    up
  else
    echo ">> Modo de operação desconhecido. Rede $NETWORK_NAME não detectada"
  fi
}

# Status
function status() {
  echo ">> Status dos containers relacionados a '$PROJECT_NAME'..."

  if [ "$MODE" = "swarm" ]; then
    docker service ls
    echo ""
  fi

  docker ps --filter "name=$PROJECT_NAME"
}

# Clean
function clean() {
  echo ">> Limpando containers, volumes e redes associadas a '$PROJECT_NAME'..."

  stop_all
  remove_jupyter
  docker volume prune -f
  docker network rm "$NETWORK_NAME" 2>/dev/null || echo "Rede '$NETWORK_NAME' não existe ou já foi removida"
}

# Help
function help() {
  cat <<EOF
Comandos disponíveis:

  build                 - Constrói a imagem do Jupyter
  deploy [n]            - Swarm: Cria rede + Inicia Jupyter + Faz deploy com n workers (padrão: 2)
  up                    - Compose: Cria rede + Inicia Jupyter + docker-compose up
  stop                  - Para Jupyter e os containers
  restart               - Reinicia Jupyter e containers
  status                - Mostra o status dos containers relacionados ao projeto
  clean                 - Remove todos os containers, volumes, redes e o Jupyter (imagem é mantida)
  help                  - Mostra este menu de ajuda

Exemplos:

  ./bin/run.sh                         # Modo swarm por padrão (rede + jupyter + stack deploy)
  ./bin/run.sh deploy 3                # Swarm com 3 workers
  ./bin/run.sh up                      # Compose com rede bridge e jupyter
  ./bin/run.sh restart                 # Reinicia o projeto
  ./bin/run.sh clean                   # Limpa tudo do projeto

URLs úteis:

  JupyterLab:    http://localhost:8888 + (token)
  Spark Master:  http://localhost:8080
  Spark UI:      http://localhost:4040

EOF
}

function print_urls() {
  echo ""
  echo "Acesso ao ambiente:"
  echo "  • Spark Master:        http://localhost:8080"
  echo "  • Spark UI (jobs):     http://localhost:4040 (disponível após alguma tarefa ser executada)"

  local token_url
  token_url=$(docker logs "$JUPYTER_NAME" 2>&1 | grep -oE "http://127\.0\.0\.1:8888[^ ]+" | tail -n 1)
  if [[ -n "$token_url" ]]; then
    token_url=${token_url/127.0.0.1/localhost}
    echo "  • JupyterLab:          $token_url"
  else
    echo "  • JupyterLab:          http://localhost:8888 (token ainda não disponível)"
  fi
  echo ""
}

### Dispatcher
detect_mode

if [ $# -eq 0 ]; then
  build
  deploy
  exit 0
fi

case "$1" in
  build)          build ;;
  deploy)         shift; deploy "$@" ;;
  up)             up ;;
  stop)           stop_all ;;
  restart)        restart ;;
  status)         status ;;
  clean)          clean ;;
  help|"")        help ;;
  *)
    echo "Comando desconhecido: $1"
    help
    exit 1
    ;;
esac