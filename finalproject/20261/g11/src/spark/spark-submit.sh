#!/usr/bin/env bash
# =========================================================================== #
# spark-submit do job de Structured Streaming
# =========================================================================== #
#
# Roda spark/stream_job.py no cluster Spark standalone do docker-compose.
# Pensado para ser executado DE DENTRO do container spark-master:
#
#     docker compose exec spark-master /app/spark-submit.sh
#
# (o diretorio ./spark do host esta montado em /app no container — ver
#  docker-compose.yml). Argumentos extras passados a este script vao direto
#  para o stream_job.py, ex.:
#
#     docker compose exec spark-master /app/spark-submit.sh \
#         --await-timeout 120 --watermark-delay "0 seconds"
#
# --------------------------------------------------------------------------- #
# Pacote do conector Kafka
# --------------------------------------------------------------------------- #
# O suporte a Kafka NAO vem embutido no Spark; e preciso adicionar o pacote
# Maven `spark-sql-kafka-0-10`. A versao DEVE casar com:
#   - a versao do Spark da imagem  -> 3.5.4
#   - a versao do Scala da imagem  -> 2.12  (bitnamilegacy/spark:3.5.4)
# Logo: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4
#
# Na 1a execucao o Spark baixa o pacote (e suas deps transitivas: kafka-clients,
# commons-pool2, spark-token-provider-kafka) do Maven Central para ~/.ivy2.
# Requer rede no container. Para uso offline/repetido, o cache ivy persiste
# enquanto o container viver.
# --------------------------------------------------------------------------- #
set -euo pipefail

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
APP="/app/stream_job.py"

# --------------------------------------------------------------------------- #
# Recursos do executor — parametrizaveis para o benchmark.
# --------------------------------------------------------------------------- #
# Cenario A varia spark.executor.cores ∈ {1,2,4}. Em modo standalone, cada
# executor recebe EXECUTOR_CORES cores; TOTAL_EXECUTOR_CORES limita o total do
# app no cluster. Mantendo TOTAL == EXECUTOR_CORES (default) o cluster sobe UM
# executor com EXECUTOR_CORES cores, isolando a variavel "cores por executor".
#   - SPARK_EXECUTOR_CORES        -> --conf spark.executor.cores
#   - SPARK_TOTAL_EXECUTOR_CORES  -> --total-executor-cores
#   - SPARK_EXECUTOR_MEMORY       -> --executor-memory
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-4}"
TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-${EXECUTOR_CORES}}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2G}"
# Alinhe shuffle partitions ao total de cores (evita overhead com poucos cores).
SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-12}"

echo "[spark-submit] executor.cores=${EXECUTOR_CORES} total-executor-cores=${TOTAL_EXECUTOR_CORES} executor-memory=${EXECUTOR_MEMORY} shuffle.partitions=${SHUFFLE_PARTITIONS}"

# Cache ivy dos --packages num diretorio PERSISTENTE e gravavel. Na imagem
# bitnami o container roda como UID 1001 com HOME=/ (nao gravavel), entao o
# ~/.ivy2 default fica vazio e cada run REbaixa os jars (e um download que trava
# derruba o run). /ivy e bind-mount rw no node-infra (fora de output/checkpoints,
# que o benchmark limpa a cada run): baixa 1x, reusa em todos os runs.
IVY_DIR="${SPARK_IVY_DIR:-/ivy}"
mkdir -p "${IVY_DIR}"

exec spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --name "nyc-tlc-stream" \
    --packages "${KAFKA_PKG}" \
    --conf spark.jars.ivy="${IVY_DIR}" \
    --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS}" \
    --conf spark.sql.streaming.metricsEnabled=true \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.executor.cores="${EXECUTOR_CORES}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --total-executor-cores "${TOTAL_EXECUTOR_CORES}" \
    "${APP}" \
    "$@"
