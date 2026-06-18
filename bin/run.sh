#!/usr/bin/env bash
#
# Sobe o pipeline inteiro e mostra os trades ao vivo no terminal.
#
#   1. garante Kafka + Postgres no ar (docker compose)
#   2. cria o tópico `trades` se ainda não existir
#   3. roda o coletor (binance.py) em background
#   4. roda o consumidor (consumer.py) em primeiro plano -> a saída que você vê
#
# Ctrl+C derruba o coletor junto e encerra limpo.
#
# Uso:  ./run.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."   # raiz do repositório (o script vive em bin/)

PY=".venv/bin/python"
BOOTSTRAP="localhost:9092"
TOPIC="trades"
PARTITIONS="${PARTITIONS:-1}"   # ./run.sh com PARTITIONS=4 para os experimentos

# --- pré-requisitos ---
[ -x "$PY" ] || { echo "venv não encontrado. Rode: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"; exit 1; }

echo "==> Subindo Kafka/Postgres (docker compose up -d)..."
docker compose up -d

echo "==> Esperando o broker ficar pronto..."
for i in $(seq 1 30); do
  if docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    echo "    broker pronto."
    break
  fi
  [ "$i" = "30" ] && { echo "    broker não respondeu a tempo."; exit 1; }
  sleep 2
done

echo "==> Garantindo o tópico '$TOPIC' ($PARTITIONS partição/ões)..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists --topic "$TOPIC" \
  --partitions "$PARTITIONS" --replication-factor 1

# --- limpeza: ao sair, mata o coletor ---
COLETOR_PID=""
cleanup() {
  echo ""
  echo "==> Encerrando..."
  [ -n "$COLETOR_PID" ] && kill "$COLETOR_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo "    pipeline parado (Kafka/Postgres continuam no ar; 'docker compose down' para derrubar)."
}
trap cleanup INT TERM EXIT

echo "==> Iniciando o coletor (binance.py) em background..."
$PY -u src/binance.py &
COLETOR_PID=$!
sleep 2  # dá um tempo pra conectar na Binance antes de começar a consumir

#echo "==> Consumidor ao vivo (Ctrl+C para parar):"
#echo "------------------------------------------------------------"
#$PY -u consumer.py

echo "==> Iniciando o Processador Spark (OHLC)..."
echo "------------------------------------------------------------"
$PY -u src/processor.py
