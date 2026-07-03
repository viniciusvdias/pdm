#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/misc/docker"
SPARK_DIR="$PROJECT_ROOT/src/spark"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/pids"
BOOTSTRAP_SCRIPT="$PROJECT_ROOT/bin/bootstrap_python_env.sh"

mkdir -p "$LOG_DIR" "$PID_DIR"

SPARK_LOG="$LOG_DIR/spark_consumer.log"
SIMULADOR_LOG="$LOG_DIR/simulador_stress2.log"
STREAMLIT_LOG="$LOG_DIR/streamlit.log"

SPARK_PID_FILE="$PID_DIR/spark_consumer.pid"
SIMULADOR_PID_FILE="$PID_DIR/simulador_stress2.pid"
STREAMLIT_PID_FILE="$PID_DIR/streamlit.pid"

POSTGRES_CONTAINER="postgres_db"
POSTGRES_USER="admin"
POSTGRES_DB="incendios_db"
STREAMLIT_PORT="8501"

ensure_file_exists() {
  local file_path="$1"
  if [[ ! -e "$file_path" ]]; then
    echo "Arquivo nao encontrado: $file_path" >&2
    exit 1
  fi
}

ensure_file_exists "$BOOTSTRAP_SCRIPT"
VENV_DIR="$("$BOOTSTRAP_SCRIPT")"
PYTHON_BIN="$VENV_DIR/bin/python3"
STREAMLIT_BIN="$VENV_DIR/bin/streamlit"

wait_for_postgres() {
  echo "Aguardando PostgreSQL ficar disponivel..."
  for _ in {1..60}; do
    if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
      echo "PostgreSQL pronto."
      return 0
    fi
    sleep 2
  done
  echo "Timeout aguardando PostgreSQL." >&2
  exit 1
}

wait_for_kafka() {
  echo "Aguardando Kafka ficar disponivel..."
  for _ in {1..60}; do
    if docker exec kafka bash -lc "kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1"; then
      echo "Kafka pronto."
      return 0
    fi
    sleep 2
  done
  echo "Timeout aguardando Kafka." >&2
  exit 1
}

ensure_table_exists() {
  echo "Garantindo a tabela historico_sensores no PostgreSQL..."
  docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE TABLE IF NOT EXISTS historico_sensores (
    sensor_id TEXT,
    timestamp TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    temperatura DOUBLE PRECISION,
    umidade DOUBLE PRECISION,
    co2 DOUBLE PRECISION,
    status_ia_borda TEXT,
    indice_risco DOUBLE PRECISION
);
SQL
}

resolve_spark_submit() {
  "$PYTHON_BIN" - <<'PY'
import os
import pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), "bin", "spark-submit"))
PY
}

start_background_process() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  shift 3

  if [[ -f "$pid_file" ]]; then
    local old_pid
    old_pid="$(cat "$pid_file")"
    if kill -0 "$old_pid" >/dev/null 2>&1; then
      echo "$name ja esta em execucao com PID $old_pid."
      return 0
    fi
    rm -f "$pid_file"
  fi

  echo "Iniciando $name..."
  nohup "$@" >"$log_file" 2>&1 &
  local new_pid=$!
  echo "$new_pid" >"$pid_file"
  sleep 3

  if kill -0 "$new_pid" >/dev/null 2>&1; then
    echo "$name iniciado com PID $new_pid."
  else
    echo "Falha ao iniciar $name. Veja o log em $log_file" >&2
    tail -n 40 "$log_file" || true
    exit 1
  fi
}

is_port_listening() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

handle_existing_streamlit() {
  if is_port_listening "$STREAMLIT_PORT"; then
    echo "Streamlit ja esta em execucao na porta $STREAMLIT_PORT. Reutilizando a instancia existente."
    local existing_pid
    existing_pid="$(lsof -tiTCP:"$STREAMLIT_PORT" -sTCP:LISTEN | head -n 1)"
    if [[ -n "$existing_pid" ]]; then
      echo "$existing_pid" >"$STREAMLIT_PID_FILE"
    fi
    return 0
  fi

  start_background_process \
    "Streamlit" \
    "$STREAMLIT_PID_FILE" \
    "$STREAMLIT_LOG" \
    "$STREAMLIT_BIN" \
    run "$PROJECT_ROOT/src/app_streamlit_monitoramento.py" \
    --server.headless true \
    --server.port "$STREAMLIT_PORT"
}

ensure_file_exists "$PYTHON_BIN"
ensure_file_exists "$STREAMLIT_BIN"
ensure_file_exists "$PROJECT_ROOT/src/simulador_stress2.py"
ensure_file_exists "$PROJECT_ROOT/src/app_streamlit_monitoramento.py"
ensure_file_exists "$SPARK_DIR/spark_consumer.py"

SPARK_SUBMIT_BIN="$(resolve_spark_submit)"
ensure_file_exists "$SPARK_SUBMIT_BIN"

echo "Subindo infraestrutura Docker..."
docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d

wait_for_postgres
wait_for_kafka
ensure_table_exists

start_background_process \
  "Spark Consumer" \
  "$SPARK_PID_FILE" \
  "$SPARK_LOG" \
  "$SPARK_SUBMIT_BIN" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3 \
  "$SPARK_DIR/spark_consumer.py"

start_background_process \
  "Simulador" \
  "$SIMULADOR_PID_FILE" \
  "$SIMULADOR_LOG" \
  "$PYTHON_BIN" \
  "$PROJECT_ROOT/src/simulador_stress2.py"

handle_existing_streamlit

echo
echo "Ambiente iniciado com sucesso."
echo "Logs:"
echo "  Spark:      $SPARK_LOG"
echo "  Streamlit:  $STREAMLIT_LOG"
echo "  Simulador:  $SIMULADOR_LOG"
echo
echo "PIDs:"
echo "  Spark:      $(cat "$SPARK_PID_FILE")"
echo "  Streamlit:  $(cat "$STREAMLIT_PID_FILE")"
echo "  Simulador:  $(cat "$SIMULADOR_PID_FILE")"
echo
echo "Acesse a interface em: http://localhost:$STREAMLIT_PORT"
echo "Para acompanhar o Spark: tail -f \"$SPARK_LOG\""
echo "Para acompanhar o simulador: tail -f \"$SIMULADOR_LOG\""
