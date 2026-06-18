#!/usr/bin/env bash
#
# Experimentos de desempenho com REPETIÇÕES (requisito: >= 3 rodadas por config).
#
# Para cada configuração (partições_janela) roda REPS vezes. Cada rodada gera um
# CSV próprio rotulado com o número da repetição, ex: performance_p2_w10_r3.csv.
# O analysis.ipynb agrega essas rodadas em média ± desvio-padrão.
#
# As rodadas usam o MESMO código do processor.py que roda no docker compose;
# aqui ele é executado a partir do host (.venv) contra o Kafka em container,
# por conveniência de medição (controle de tempo e variáveis por rodada).
#
# Uso:
#   ./experiment.sh                       # todas as configs, 3 repetições, 120s cada
#   REPS=5 DURATION=60 ./experiment.sh    # 5 repetições de 60s
#   EXPERIMENTS="1_10 2_10 4_10" ./experiment.sh   # só a varredura de partições
#
set -euo pipefail
cd "$(dirname "$0")/.."   # raiz do repositório (o script vive em bin/)

PY=".venv/bin/python"
BOOTSTRAP="localhost:9092"
TOPIC="trades"
DURATION="${DURATION:-120}"   # segundos por rodada
REPS="${REPS:-3}"             # repetições por configuração (mínimo do enunciado: 3)

# Configurações: "PARTICOES_JANELA". A varredura de partições é o experimento central.
ALL_EXPERIMENTS="1_10 2_10 4_10 1_5 1_30"
EXPERIMENTS="${EXPERIMENTS:-$ALL_EXPERIMENTS}"

[ -x "$PY" ] || { echo "venv não encontrado. Rode: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"; exit 1; }
docker exec kafka true 2>/dev/null || { echo "container 'kafka' não está no ar. Rode: docker compose up -d kafka"; exit 1; }

mkdir -p metrics

echo "============================================================"
echo "  EXPERIMENTOS DE DESEMPENHO"
echo "  Repetições por config: $REPS | Duração por rodada: ${DURATION}s"
echo "  Configurações: $EXPERIMENTS"
echo "============================================================"

for exp in $EXPERIMENTS; do
    PARTITIONS=$(echo "$exp" | cut -d_ -f1)
    WINDOW_SIZE=$(echo "$exp" | cut -d_ -f2)

    for r in $(seq 1 "$REPS"); do
        LABEL="p${PARTITIONS}_w${WINDOW_SIZE}_r${r}"

        echo ""
        echo ">>> $LABEL  (partições=$PARTITIONS, janela=${WINDOW_SIZE}s, rodada $r/$REPS)"
        echo "------------------------------------------------------------"

        # 1. Recria o tópico com o número de partições da config (estado limpo por rodada)
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --delete --topic "$TOPIC" 2>/dev/null || true
        sleep 3
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
        sleep 2

        # 2. Remove CSVs anteriores desta rodada (re-execução limpa)
        rm -f "metrics/performance_${LABEL}.csv" "metrics/candles_${LABEL}.csv"

        # 3. Coletor em background
        $PY -u src/binance.py &
        COLETOR_PID=$!
        sleep 3

        # 4. Processador por DURATION segundos, com as variáveis da rodada
        WINDOW_SIZE_SECONDS="$WINDOW_SIZE" \
        WATERMARK_SECONDS="$WINDOW_SIZE" \
        EXPERIMENT_LABEL="$LABEL" \
        PARTITIONS="$PARTITIONS" \
        timeout "$DURATION" $PY -u src/processor.py || true

        # 5. Encerra o coletor
        kill "$COLETOR_PID" 2>/dev/null || true
        wait "$COLETOR_PID" 2>/dev/null || true

        echo "    -> metrics/performance_${LABEL}.csv"
        sleep 4  # deixa o Kafka estabilizar entre rodadas
    done
done

echo ""
echo "============================================================"
echo "  CONCLUÍDO. Resultados em metrics/ (1 CSV por rodada)."
echo "  Abra analysis.ipynb para a estatística (média ± desvio)."
echo "============================================================"
