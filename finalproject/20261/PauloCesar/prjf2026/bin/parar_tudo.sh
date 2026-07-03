#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/misc/docker"
PID_DIR="$PROJECT_ROOT/pids"

SPARK_PID_FILE="$PID_DIR/spark_consumer.pid"
SIMULADOR_PID_FILE="$PID_DIR/simulador_stress2.pid"
STREAMLIT_PID_FILE="$PID_DIR/streamlit.pid"

stop_process_from_pid_file() {
  local name="$1"
  local pid_file="$2"

  if [[ ! -f "$pid_file" ]]; then
    echo "$name: arquivo de PID nao encontrado, nada para encerrar."
    return 0
  fi

  local pid
  pid="$(cat "$pid_file")"

  if [[ -z "$pid" ]]; then
    echo "$name: arquivo de PID vazio, removendo."
    rm -f "$pid_file"
    return 0
  fi

  if kill -0 "$pid" >/dev/null 2>&1; then
    echo "Encerrando $name (PID $pid)..."
    kill "$pid" >/dev/null 2>&1 || true

    for _ in {1..10}; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done

    if kill -0 "$pid" >/dev/null 2>&1; then
      echo "$name ainda ativo, forcando encerramento..."
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi

    echo "$name encerrado."
  else
    echo "$name: processo $pid nao estava mais em execucao."
  fi

  rm -f "$pid_file"
}

echo "Encerrando processos locais..."
stop_process_from_pid_file "Spark Consumer" "$SPARK_PID_FILE"
stop_process_from_pid_file "Simulador" "$SIMULADOR_PID_FILE"
stop_process_from_pid_file "Streamlit" "$STREAMLIT_PID_FILE"

echo
echo "Derrubando containers Docker do projeto..."
docker compose -f "$DOCKER_DIR/docker-compose.yml" down

echo
echo "Ambiente encerrado com sucesso."
