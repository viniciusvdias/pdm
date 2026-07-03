#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
BOOTSTRAP_SCRIPT="$PROJECT_ROOT/bin/bootstrap_python_env.sh"

if [[ ! -x "$BOOTSTRAP_SCRIPT" ]]; then
  echo "Script de bootstrap nao encontrado: $BOOTSTRAP_SCRIPT" >&2
  exit 1
fi

VENV_DIR="$("$BOOTSTRAP_SCRIPT")"
PYTHON_BIN="$VENV_DIR/bin/python3"
KAFKA_LOG_CMD="docker exec -it kafka bash -lc 'kafka-console-consumer --bootstrap-server localhost:9092 --topic dados-sensores --property print.timestamp=true'"
SPARK_LOG_CMD="cd \"$PROJECT_ROOT\" && mkdir -p \"$LOG_DIR\" && touch \"$LOG_DIR/spark_consumer.log\" && tail -f \"$LOG_DIR/spark_consumer.log\""
SIMULADOR_LOG_CMD="cd \"$PROJECT_ROOT\" && mkdir -p \"$LOG_DIR\" && touch \"$LOG_DIR/simulador_stress2.log\" && tail -f \"$LOG_DIR/simulador_stress2.log\""
POSTGRES_LOG_CMD="cd \"$PROJECT_ROOT\" && \"$PYTHON_BIN\" \"$PROJECT_ROOT/src/monitor_postgres_tempo_real.py\""

open_terminal_window() {
  local command="$1"
  local escaped_command="${command//\\/\\\\}"
  escaped_command="${escaped_command//\"/\\\"}"
  osascript <<OSA
tell application "Terminal"
    activate
    do script "$escaped_command"
end tell
OSA
}

echo "Abrindo janelas de observacao do pipeline..."

open_terminal_window "$KAFKA_LOG_CMD"
sleep 1
open_terminal_window "$SPARK_LOG_CMD"
sleep 1
open_terminal_window "$POSTGRES_LOG_CMD"
sleep 1
open_terminal_window "$SIMULADOR_LOG_CMD"

echo
echo "Janelas abertas:"
echo "  1. Kafka recebendo mensagens do topico dados-sensores"
echo "  2. Spark reagindo aos lotes em logs/spark_consumer.log"
echo "  3. PostgreSQL mostrando os registros gravados em tempo real"
echo "  4. Simulador gerando carga em logs/simulador_stress2.log"
echo
echo "Se ainda nao estiver rodando, inicie primeiro o ambiente com:"
echo "  ./bin/iniciar_tudo.sh"
