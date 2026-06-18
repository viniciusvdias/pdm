#!/usr/bin/env bash
#
# Roda experimentos automatizados variando partições e tamanho de janela.
#
# Cada rodada:
#   1. Deleta e recria o tópico com N partições
#   2. Roda o pipeline por DURATION segundos
#   3. Salva CSVs em metrics/ com label da configuração
#
# Uso:
#   ./experiment.sh                    # roda todos os experimentos
#   DURATION=60 ./experiment.sh        # cada rodada dura 60s (default: 120s)
#   EXPERIMENTS="1_10" ./experiment.sh  # roda só 1 partição, janela 10s
#
set -euo pipefail
cd "$(dirname "$0")"

PY="venv/bin/python"
BOOTSTRAP="localhost:9092"
TOPIC="trades"
DURATION="${DURATION:-120}"  # segundos por experimento

# Configurações: "PARTITIONS_WINDOWSIZE"
ALL_EXPERIMENTS="1_10 2_10 4_10 1_5 1_30"
EXPERIMENTS="${EXPERIMENTS:-$ALL_EXPERIMENTS}"

[ -x "$PY" ] || { echo "venv não encontrado."; exit 1; }

echo "============================================================"
echo "  EXPERIMENTOS DE DESEMPENHO"
echo "  Duração por rodada: ${DURATION}s"
echo "  Configurações: $EXPERIMENTS"
echo "============================================================"

for exp in $EXPERIMENTS; do
    PARTITIONS=$(echo "$exp" | cut -d_ -f1)
    WINDOW_SIZE=$(echo "$exp" | cut -d_ -f2)
    LABEL="p${PARTITIONS}_w${WINDOW_SIZE}"

    echo ""
    echo ">>> EXPERIMENTO: $LABEL (partições=$PARTITIONS, janela=${WINDOW_SIZE}s)"
    echo "------------------------------------------------------------"

    # 1. Recria o tópico com o número correto de partições
    echo "    Deletando tópico antigo..."
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --delete --topic "$TOPIC" 2>/dev/null || true
    sleep 3

    echo "    Criando tópico com $PARTITIONS partição(ões)..."
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --create --topic "$TOPIC" \
        --partitions "$PARTITIONS" --replication-factor 1
    sleep 2

    # 2. Limpa CSVs anteriores desta config (se existirem)
    rm -f "metrics/performance_${LABEL}.csv" "metrics/candles_${LABEL}.csv"

    # 3. Inicia o coletor em background
    echo "    Iniciando coletor..."
    $PY -u binance.py &
    COLETOR_PID=$!
    sleep 3

    # 4. Inicia o processador em background com as variáveis do experimento
    echo "    Iniciando processador (${DURATION}s)..."
    WINDOW_SIZE_SECONDS="$WINDOW_SIZE" \
    WATERMARK_SECONDS="$WINDOW_SIZE" \
    EXPERIMENT_LABEL="$LABEL" \
    PARTITIONS="$PARTITIONS" \
    timeout "$DURATION" $PY -u processor.py &
    PROC_PID=$!

    # 5. Espera a duração do experimento
    wait $PROC_PID 2>/dev/null || true

    # 6. Mata o coletor
    kill $COLETOR_PID 2>/dev/null || true
    wait $COLETOR_PID 2>/dev/null || true

    echo "    Experimento $LABEL concluído."
    echo "    -> metrics/performance_${LABEL}.csv"
    echo "    -> metrics/candles_${LABEL}.csv"

    # Pausa entre experimentos para o Kafka estabilizar
    sleep 5
done

echo ""
echo "============================================================"
echo "  TODOS OS EXPERIMENTOS CONCLUÍDOS"
echo "  Resultados em: metrics/"
echo "  Abra analysis.ipynb para gerar os gráficos"
echo "============================================================"
