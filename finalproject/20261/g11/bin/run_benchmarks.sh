#!/usr/bin/env bash
# =========================================================================== #
# Harness de benchmark do pipeline de stream processing (NYC TLC)
# =========================================================================== #
#
# Mede THROUGHPUT (registros/s) e MICRO-BATCH LATENCY (ms) do PySpark Structured
# Streaming variando dois fatores, com >=3 repeticoes cada e media+desvio:
#
#   Cenario A  spark.executor.cores ∈ {1,2,4}   (numero de workers FIXO)
#   Cenario B  spark-worker          ∈ {1,2,3}   (cores por executor FIXO)
#
# Metrica: cada run grava StreamingQueryProgress por micro-batch (via o
# StreamingQueryListener do stream_job.py, flag --metrics-file). O parser
# (parse_metrics.py) resume cada run e depois agrega por configuracao.
#
# Ciclo de UM run:
#   1. limpa topico (recria), output e checkpoints (estado zerado, comparavel);
#   2. reconfigura o cluster (escala workers / define cores do executor);
#   3. sobe o producer com um VOLUME FIXO de mensagens (--max-records) e espera;
#   4. roda o stream_job ate drenar o backlog (--idle-stop) ou estourar o teto;
#   5. coleta as metricas (parse_metrics.py run) e anexa ao runs.csv;
#   6. encerra o job (o spark-submit ja sai sozinho ao drenar).
#
# Uso:
#   benchmarks/run_benchmarks.sh                 # roda A e B (completo)
#   SCENARIOS="A" benchmarks/run_benchmarks.sh   # so o cenario A
#   SCENARIOS="B" REPS=3 benchmarks/run_benchmarks.sh
#   MAX_RECORDS=300000 REPS=1 SCENARIOS="A" benchmarks/run_benchmarks.sh  # validacao rapida
#
# Variaveis de ambiente (com defaults):
#   SCENARIOS   "A B"          quais cenarios rodar
#   REPS        3              repeticoes por configuracao
#   MAX_RECORDS 1000000        volume fixo de mensagens por run (producer)
#   DATA_DIR    ./data          diretorio dos parquet (ex.: ./data_subset p/ 45M)
#   A_CORES     "1 2 4"        valores de spark.executor.cores (cenario A)
#   A_WORKERS   1              numero de workers fixo no cenario A
#   B_WORKERS   "1 2 3"        numero de workers (cenario B)
#   B_CORES     4              spark.executor.cores fixo no cenario B
#   TRIGGER     "5 seconds"    intervalo do micro-batch
#   AWAIT_TIMEOUT 600          teto de tempo (s) do job por run
#   IDLE_STOP   3              polls ociosos (de 5s) p/ considerar backlog drenado
# =========================================================================== #
set -euo pipefail

# --------------------------------------------------------------------------- #
# Caminhos / config
# --------------------------------------------------------------------------- #
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
cd "${ROOT}"

# Compose vive em misc/ (estrutura de entrega g11/). Todos os `docker compose`
# deste script passam por este alias para achar o arquivo certo.
COMPOSE=(docker compose -f "${ROOT}/misc/docker-compose.yml")

# parse_metrics.py foi para src/benchmarks/ (estrutura de entrega g11/).
BENCH_SRC="${ROOT}/src/benchmarks"

RESULTS_DIR="${ROOT}/misc/results"
RUNS_CSV="${RESULTS_DIR}/runs.csv"
SUMMARY_CSV="${RESULTS_DIR}/summary.csv"
METRICS_DIR="${RESULTS_DIR}/metrics"      # 1 JSONL por run
mkdir -p "${RESULTS_DIR}" "${METRICS_DIR}"

# Parametros (overrideaveis por env).
SCENARIOS="${SCENARIOS:-A B}"
REPS="${REPS:-3}"
MAX_RECORDS="${MAX_RECORDS:-1000000}"
A_CORES="${A_CORES:-1 2 4}"
A_WORKERS="${A_WORKERS:-1}"
B_WORKERS="${B_WORKERS:-1 2 3}"
B_CORES="${B_CORES:-4}"
TRIGGER="${TRIGGER:-5 seconds}"
AWAIT_TIMEOUT="${AWAIT_TIMEOUT:-600}"
IDLE_STOP="${IDLE_STOP:-3}"

TOPIC="${KAFKA_TOPIC:-taxi_trips_stream}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-12}"

# Caminho do metrics-file DENTRO do container (montado em /output). Usamos /output
# pois ja e um volume rw compartilhado host<->container; o host le o mesmo arquivo.
METRICS_IN_CONTAINER="/output/bench_metrics.jsonl"
METRICS_ON_HOST="${ROOT}/output/bench_metrics.jsonl"

# Python do venv (producer + parser rodam no host).
PY="python"
if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
fi

log() { echo -e "\n\033[1;36m[bench] $*\033[0m"; }

# --------------------------------------------------------------------------- #
# Helpers de ciclo de vida
# --------------------------------------------------------------------------- #

reset_state() {
  # Zera output, checkpoints e RECRIA o topico — cada run parte do mesmo estado,
  # senao o startingOffsets=earliest + checkpoint reaproveitariam dados/offsets.
  log "reset: limpando output/, checkpoints/ e recriando topico ${TOPIC}"
  rm -rf "${ROOT}/output/"* "${ROOT}/checkpoints/"* 2>/dev/null || true
  rm -f "${METRICS_ON_HOST}" 2>/dev/null || true

  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --delete --topic "${TOPIC}" 2>/dev/null || true
  # pequena espera para a delecao propagar
  sleep 3
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "${TOPIC}" --partitions "${PARTITIONS}" --replication-factor 1
}

scale_workers() {
  # Reescala o numero de workers e espera todos ficarem ALIVE.
  local n="$1"
  log "escalando spark-worker=${n}"
  "${COMPOSE[@]}" up -d --scale "spark-worker=${n}" --no-recreate spark-worker
  # espera o master enxergar n workers ALIVE
  local tries=0
  while :; do
    local alive
    alive="$(curl -s http://localhost:8080/json/ | "${PY}" -c \
      'import sys,json; print(json.load(sys.stdin).get("aliveworkers",0))' 2>/dev/null || echo 0)"
    if [[ "${alive}" == "${n}" ]]; then
      log "workers ALIVE = ${alive}"
      break
    fi
    tries=$((tries+1))
    if (( tries > 30 )); then
      log "AVISO: esperado ${n} workers ALIVE, vendo ${alive} (seguindo mesmo assim)"
      break
    fi
    sleep 2
  done
}

produce_fixed_volume() {
  # Sobe o producer (container, 100% Docker) com volume fixo de mensagens e
  # ESPERA terminar. DATA_DIR_HOST (env, default ../datasample) permite apontar
  # para o dataset completo (ex.: DATA_DIR_HOST=../data cobre os 4 service_types
  # em escala). O producer le KAFKA_BOOTSTRAP=kafka:9092 e DATA_DIR=/data do env
  # do servico `producer` no compose.
  log "producer: enviando ${MAX_RECORDS} mensagens (--interleave) via container"
  "${COMPOSE[@]}" --profile ingest run --rm producer \
    --max-records "${MAX_RECORDS}" --interleave
}

run_stream_job() {
  # Roda o job de streaming ate drenar o backlog. Bloqueante (sai ao terminar).
  #   $1 = spark.executor.cores
  #   $2 = total-executor-cores (default = $1; cenario B passa workers*cores)
  local exec_cores="$1"
  local total_cores="${2:-$1}"
  log "stream_job: executor.cores=${exec_cores}, total=${total_cores}, trigger='${TRIGGER}', drenando backlog"
  "${COMPOSE[@]}" exec -T \
    -e SPARK_EXECUTOR_CORES="${exec_cores}" \
    -e SPARK_TOTAL_EXECUTOR_CORES="${total_cores}" \
    spark-master /app/spark-submit.sh \
      --trigger "${TRIGGER}" \
      --watermark-delay "0 seconds" \
      --starting-offsets earliest \
      --await-timeout "${AWAIT_TIMEOUT}" \
      --idle-stop "${IDLE_STOP}" \
      --metrics-file "${METRICS_IN_CONTAINER}"
}

collect_run() {
  # Resume o JSONL deste run e anexa ao runs.csv. Tambem arquiva o JSONL.
  local run_id="$1" label="$2"
  local archived="${METRICS_DIR}/${run_id}.jsonl"
  if [[ -f "${METRICS_ON_HOST}" ]]; then
    cp "${METRICS_ON_HOST}" "${archived}"
  else
    log "AVISO: metrics-file nao encontrado em ${METRICS_ON_HOST} (run ${run_id})"
    : > "${archived}"
  fi
  ${PY} "${BENCH_SRC}/parse_metrics.py" run \
    --run-id "${run_id}" --label "${label}" \
    --runs-csv "${RUNS_CSV}" \
    "${archived}"
}

one_run() {
  # Executa um run completo: reset -> scale -> produce -> stream -> collect.
  #   $1 scenario  $2 param  $3 value  $4 workers  $5 exec_cores  $6 rep
  #   $7 total_cores (opcional; default = exec_cores)
  local scenario="$1" param="$2" value="$3" workers="$4" exec_cores="$5" rep="$6"
  local total_cores="${7:-$5}"
  local run_id="${scenario}_${param}${value}_w${workers}_rep${rep}"
  local label="scenario=${scenario},param=${param},value=${value},workers=${workers},rep=${rep}"

  log "===== RUN ${run_id} ====="
  reset_state
  scale_workers "${workers}"
  produce_fixed_volume
  run_stream_job "${exec_cores}" "${total_cores}" || log "AVISO: stream_job retornou nao-zero (run ${run_id})"
  collect_run "${run_id}" "${label}"
}

# --------------------------------------------------------------------------- #
# Cenarios
# --------------------------------------------------------------------------- #
scenario_A() {
  log "########## CENARIO A — variando spark.executor.cores (workers=${A_WORKERS}) ##########"
  for cores in ${A_CORES}; do
    for rep in $(seq 1 "${REPS}"); do
      one_run "A" "cores" "${cores}" "${A_WORKERS}" "${cores}" "${rep}"
    done
  done
}

scenario_B() {
  log "########## CENARIO B — variando spark-worker (cores=${B_CORES}) ##########"
  for workers in ${B_WORKERS}; do
    for rep in $(seq 1 "${REPS}"); do
      # No cenario B mantemos cores por executor fixo (B_CORES) e o
      # total-executor-cores escala com o n de workers (workers*B_CORES) para que
      # o app possa de fato usar todos os workers.
      local total=$(( workers * B_CORES ))
      one_run "B" "workers" "${workers}" "${workers}" "${B_CORES}" "${rep}" "${total}"
    done
  done
}

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
log "Iniciando benchmark | SCENARIOS='${SCENARIOS}' REPS=${REPS} MAX_RECORDS=${MAX_RECORDS}"
log "resultados em ${RESULTS_DIR}"

# Garante infra basica no ar (kafka + master). Workers sao escalados por run.
"${COMPOSE[@]}" up -d kafka kafka-init spark-master >/dev/null

for s in ${SCENARIOS}; do
  case "${s}" in
    A) scenario_A ;;
    B) scenario_B ;;
    *) log "cenario desconhecido: ${s} (ignorado)" ;;
  esac
done

# Agregacao final: runs.csv -> summary.csv (media + desvio por configuracao).
log "agregando resultados (media + desvio padrao)"
${PY} "${BENCH_SRC}/parse_metrics.py" aggregate --runs-csv "${RUNS_CSV}" --out "${SUMMARY_CSV}"

log "PRONTO. runs em ${RUNS_CSV} | resumo em ${SUMMARY_CSV}"
