#!/usr/bin/env bash
#
#
set -euo pipefail
cd "$(dirname "$0")/.."   # raiz do repositório

BOOTSTRAP="localhost:9092"
TOPIC="trades"
DURATION="${DURATION:-960}"   # segundos por rodada
REPS="${REPS:-3}"             # repetições por configuração

ALL_EXPERIMENTS="1_15 1_30 1_60 5_15 5_30 5_60 10_15 10_30 10_60"
EXPERIMENTS="${EXPERIMENTS:-$ALL_EXPERIMENTS}"

# Garante que os containers órfãos sejam limpos se apertar Ctrl+C no meio do experimento
trap 'echo "Interrompido! Limpando containers..."; sudo docker stop exp_processor >/dev/null 2>&1 || true; sudo docker rm exp_processor >/dev/null 2>&1 || true; sudo docker compose stop collector >/dev/null 2>&1 || true; exit 1' SIGINT SIGTERM

echo "============================================================"
echo "  EXPERIMENTOS DE DESEMPENHO"
echo "  Repetições por config: $REPS | Duração por rodada: ${DURATION}s"
echo "  Configurações: $EXPERIMENTS"
echo "============================================================"

# Prepara o terreno: derruba tudo e sobe apenas o Kafka
sudo docker compose down
echo "Iniciando Kafka..."
sudo docker compose up -d kafka
sleep 10 # Aguarda o Kafka e o KRaft inicializarem corretamente

mkdir -p metrics

# Fase de Warmup (Aquecimento)
# O Spark precisa baixar os pacotes do Kafka na primeira vez.
echo ">>> Executando Warmup do Spark (baixando dependências no container)..."
sudo docker compose run -d --name exp_warmup processor >/dev/null 2>&1
sleep 30
sudo docker stop exp_warmup >/dev/null 2>&1 || true
sudo docker rm exp_warmup >/dev/null 2>&1 || true
echo ">>> Warmup concluído."

for r in $(seq 1 "$REPS"); do
    echo ""
    echo "============================================================"
    echo ">>> INICIANDO RODADA GERAL $r DE $REPS"
    echo "============================================================"

    for exp in $EXPERIMENTS; do
        PARTITIONS=$(echo "$exp" | cut -d_ -f1)
        WINDOW_SIZE=$(echo "$exp" | cut -d_ -f2)

        LABEL="p${PARTITIONS}_w${WINDOW_SIZE}_r${r}"

        # Captura o horário exato do início desta rodada
        TEST_TIME=$(date '+%Y-%m-%d %H:%M:%S')

        echo ""
        echo ">>> $LABEL  (partições=$PARTITIONS, janela=${WINDOW_SIZE}s, rodada $r/$REPS)"
        echo "    Horário de Início: $TEST_TIME"
        echo "------------------------------------------------------------"

        # Recria o tópico dentro do container do Kafka
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --delete --topic "$TOPIC" 2>/dev/null || true
        sleep 3
        sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
        sleep 2

        # Remove CSVs anteriores desta rodada
        sudo docker run --rm -v "$(pwd)/metrics:/app/metrics" python:3.12-slim \
            rm -f "/app/metrics/performance_${LABEL}.csv" "/app/metrics/candles_${LABEL}.csv"

        # Inicia o coletor em background usando o serviço do compose
        sudo docker compose up -d collector
        sleep 3

        # Inicia o processador via 'sudo docker compose run' (injeta as variáveis de ambiente)
        sudo docker compose run -d --name exp_processor \
            -e WINDOW_SIZE_SECONDS="$WINDOW_SIZE" \
            -e WATERMARK_SECONDS="$WINDOW_SIZE" \
            -e EXPERIMENT_LABEL="$LABEL" \
            -e PARTITIONS="$PARTITIONS" \
            -e TEST_TIME="$TEST_TIME" \
            processor >/dev/null

        # Aguarda a duração do experimento
        echo "    Coletando e processando dados por ${DURATION}s..."
        sleep "$DURATION"

        # Encerra e remove o processador temporário, e para o coletor
        sudo docker stop exp_processor >/dev/null 2>&1 || true
        sudo docker rm exp_processor >/dev/null 2>&1 || true
        sudo docker compose stop collector >/dev/null 2>&1 || true

        echo "    -> Salvo em metrics/performance_${LABEL}.csv"
        sleep 4  # Deixa o broker Kafka estabilizar entre as rodadas
    done
done

echo ""
echo "============================================================"
echo "  CONCLUÍDO. Todos os experimentos finalizaram com sucesso!"
echo "  Abra o Jupyter (via sudo Docker ou local) para analisar os CSVs."
echo "============================================================"