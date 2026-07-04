#!/usr/bin/env bash
# =========================================================================== #
# Harness de benchmark do pipeline de stream processing (AWS/Swarm)
# =========================================================================== #
#
# Versao SWARM do benchmarks/run_benchmarks.sh (host unico). Roda NO node-infra
# (manager do Swarm), onde estao: o container kafka (porta EXTERNAL publicada em
# localhost:29092), o spark-master (UI em localhost:8080) e os bind-mounts
# output/ e checkpoints/ (em /home/ubuntu/taxi). Os spark-workers rodam nas 3
# VMs worker (1 replica por VM, via max_replicas_per_node=1 no docker-stack.yml).
#
# MESMA logica do original (reset -> scale -> produce -> stream -> collect) e o
# MESMO parse_metrics.py. As unicas trocas sao os comandos que assumiam host
# unico:
#
#   | Acao               | Host unico (compose)                 | Swarm (este script)                                   |
#   |--------------------|--------------------------------------|-------------------------------------------------------|
#   | Escalar workers    | docker compose up --scale spark-...  | docker service scale <stack>_spark-worker=N           |
#   | Submeter o job     | docker compose exec spark-master ... | docker exec $(docker ps -q -f name=<stack>_spark-...) |
#   | Reset do topico    | docker compose exec kafka ...        | docker exec $(docker ps -q -f name=<stack>_kafka) ... |
#   | Producer           | --bootstrap localhost:29092          | --bootstrap localhost:29092 (Kafka publicado no infra)|
#   | Esperar ALIVE      | curl localhost:8080/json/            | curl localhost:8080/json/ (spark-master UI no infra)  |
#   | Metricas (JSONL)   | ./output/bench_metrics.jsonl         | /home/ubuntu/taxi/output/bench_metrics.jsonl          |
#
# Cenarios (ajustados para a t3.medium — 2 vCPU por VM worker):
#   Cenario A  spark.executor.cores ∈ {1,2}   (numero de workers FIXO)
#   Cenario B  spark-worker          ∈ {1,2,3} (cores por executor FIXO) <- foco
#
# Uso (NO node-infra):
#   benchmarks/run_benchmarks_swarm.sh                 # roda A e B (completo)
#   SCENARIOS="B" benchmarks/run_benchmarks_swarm.sh   # so o cenario B (foco)
#   MAX_RECORDS=300000 REPS=1 SCENARIOS="A" benchmarks/run_benchmarks_swarm.sh  # smoke
#
# Variaveis de ambiente (com defaults):
#   STACK       taxi           nome da stack (docker stack deploy -c ... <STACK>)
#   REMOTE_DIR  /home/ubuntu/taxi   raiz dos arquivos no node-infra
#   SCENARIOS   "A B"          quais cenarios rodar
#   REPS        3              repeticoes por configuracao
#   MAX_RECORDS 1000000        volume fixo de mensagens por run (producer)
#   DATA_DIR    <REMOTE_DIR>/data_subset   parquet lido pelo producer
#   A_CORES     "1 2"          spark.executor.cores (cenario A) — t3.medium=2 vCPU
#   A_WORKERS   1              numero de workers fixo no cenario A
#   B_WORKERS   "1 2 3"        numero de workers (cenario B) — 1 por VM worker
#   B_CORES     2              spark.executor.cores fixo no cenario B (2 vCPU/VM)
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

# No node-infra os bind-mounts do spark-master apontam para REMOTE_DIR (output/,
# checkpoints/). O JSONL de metricas e escrito pelo driver em /output (container)
# = REMOTE_DIR/output (host do infra), que e onde este script le.
STACK="${STACK:-taxi}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/taxi}"

RESULTS_DIR="${HERE}/results"
RUNS_CSV="${RESULTS_DIR}/runs.csv"
SUMMARY_CSV="${RESULTS_DIR}/summary.csv"
METRICS_DIR="${RESULTS_DIR}/metrics"      # 1 JSONL por run
mkdir -p "${RESULTS_DIR}" "${METRICS_DIR}"

# Parametros (overrideaveis por env). Ajustados para a t3.medium (2 vCPU/VM):
#   A_CORES ∈ {1,2} (a VM comporta ate 2); B_CORES=2; B_WORKERS ∈ {1,2,3} = foco.
SCENARIOS="${SCENARIOS:-A B}"
REPS="${REPS:-3}"
MAX_RECORDS="${MAX_RECORDS:-1000000}"
A_CORES="${A_CORES:-1 2}"
A_WORKERS="${A_WORKERS:-1}"
B_WORKERS="${B_WORKERS:-1 2 3}"
B_CORES="${B_CORES:-2}"
TRIGGER="${TRIGGER:-5 seconds}"
AWAIT_TIMEOUT="${AWAIT_TIMEOUT:-600}"
IDLE_STOP="${IDLE_STOP:-3}"

# Nome-base do topico. Cada run usa um topico UNICO derivado dele (ver one_run):
# evita o bug de offsets stale ao recriar topico de mesmo nome no KRaft.
BASE_TOPIC="${KAFKA_TOPIC:-taxi_trips_stream}"
TOPIC="${BASE_TOPIC}"   # sobrescrito por run em one_run()
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-12}"

# Nomes de servico do Swarm (prefixados pelo nome da stack).
SVC_WORKER="${STACK}_spark-worker"
SVC_MASTER="${STACK}_spark-master"
SVC_KAFKA="${STACK}_kafka"

# Caminho do metrics-file DENTRO do container (montado em /output) e no host do
# node-infra (REMOTE_DIR/output — bind-mount do spark-master).
METRICS_IN_CONTAINER="/output/bench_metrics.jsonl"
METRICS_ON_HOST="${REMOTE_DIR}/output/bench_metrics.jsonl"

# Python do venv (producer + parser rodam no HOST do node-infra).
PY="python"
if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
fi

log() { echo -e "\n\033[1;36m[bench] $*\033[0m"; }

# --------------------------------------------------------------------------- #
# Helpers de Swarm: localizar o container de um servico NESTE no (o node-infra
# roda kafka e spark-master). `docker ps -q -f name=<svc>` casa o nome da task
# (<svc>.<slot>.<id>) da replica local.
# --------------------------------------------------------------------------- #
container_id() {
  # $1 = nome do servico (ex.: taxi_kafka). Ecoa o ID do container local (ou vazio).
  docker ps -q -f "name=$1" | head -n1
}

wait_for_container() {
  # Espera um container do servico $1 aparecer neste no (kafka/master sobem no infra).
  local svc="$1" tries=0 cid
  while :; do
    cid="$(container_id "${svc}")"
    if [[ -n "${cid}" ]]; then echo "${cid}"; return 0; fi
    tries=$((tries+1))
    if (( tries > 60 )); then
      log "ERRO: container do servico ${svc} nao apareceu neste no (60s)"
      return 1
    fi
    sleep 1
  done
}

# --------------------------------------------------------------------------- #
# Helpers de ciclo de vida (mesma logica; comandos Swarm)
# --------------------------------------------------------------------------- #

reset_state() {
  # Zera output, checkpoints e RECRIA o topico — cada run parte do mesmo estado,
  # senao o startingOffsets=earliest + checkpoint reaproveitariam dados/offsets.
  log "reset: limpando output/, checkpoints/ e recriando topico ${TOPIC}"
  rm -rf "${REMOTE_DIR}/output/"* "${REMOTE_DIR}/checkpoints/"* 2>/dev/null || true
  rm -f "${METRICS_ON_HOST}" 2>/dev/null || true

  local kafka_cid
  kafka_cid="$(wait_for_container "${SVC_KAFKA}")"
  local ktopics="docker exec -i ${kafka_cid} /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092"

  # ${TOPIC} e UNICO por run (definido em one_run), entao nao existe ainda: apenas
  # CRIA (sem delete). Isso evita totalmente o bug de offsets stale do KRaft ao
  # recriar topico homonimo, e e mais rapido (nada de delecao assincrona).
  ${ktopics} --create --if-not-exists \
    --topic "${TOPIC}" --partitions "${PARTITIONS}" --replication-factor 1

  # Espera as PARTITIONS particoes estarem descritas (topico pronto p/ producer).
  local i
  for i in $(seq 1 30); do
    local nparts
    nparts="$(${ktopics} --describe --topic "${TOPIC}" 2>/dev/null | grep -c 'Partition:')"
    [[ "${nparts}" -ge "${PARTITIONS}" ]] && { log "topico pronto (${nparts} particoes)"; break; }
    sleep 2
  done
}

alive_workers() {
  # Ecoa quantos workers estao ALIVE, consultando a UI do master.
  # NAO usar `curl localhost:8080` do HOST: no Swarm a porta publicada passa pelo
  # ingress mesh e o /json/ costuma vir vazio/instavel. Em vez disso, exec no
  # container do master e consulta a UI no localhost DELE (Jetty bind direto).
  local master_cid
  master_cid="$(container_id "${SVC_MASTER}")"
  [[ -z "${master_cid}" ]] && { echo 0; return; }
  docker exec -i "${master_cid}" python3 -c \
    'import urllib.request,json; print(json.load(urllib.request.urlopen("http://localhost:8080/json/",timeout=6))["aliveworkers"])' \
    2>/dev/null || echo 0
}

scale_workers() {
  # Reescala o numero de workers (1 replica por VM worker) e espera todos ALIVE.
  # Com max_replicas_per_node=1 e 3 VMs worker, N ∈ {1,2,3} cai em VMs distintas.
  local n="$1"
  log "escalando ${SVC_WORKER}=${n}"
  docker service scale --detach "${SVC_WORKER}=${n}"
  # espera o master enxergar n workers ALIVE (consulta via exec no master).
  local tries=0
  while :; do
    local alive
    alive="$(alive_workers)"
    if [[ "${alive}" == "${n}" ]]; then
      log "workers ALIVE = ${alive}"
      break
    fi
    tries=$((tries+1))
    if (( tries > 60 )); then
      log "AVISO: esperado ${n} workers ALIVE, vendo ${alive} (seguindo mesmo assim)"
      break
    fi
    sleep 2
  done
}

produce_fixed_volume() {
  # Sobe o producer com volume fixo de mensagens e ESPERA terminar. Roda no HOST
  # do node-infra apontando para localhost:29092 (Kafka EXTERNAL publicado la).
  # DATA_DIR (env) permite apontar para o subconjunto fixo (default data_subset).
  local data_dir="${DATA_DIR:-${REMOTE_DIR}/data_subset}"
  log "producer: enviando ${MAX_RECORDS} mensagens de ${data_dir} (--interleave)"
  ${PY} "${ROOT}/producer/producer.py" \
    --data-dir "${data_dir}" \
    --max-records "${MAX_RECORDS}" --interleave \
    --bootstrap localhost:29092
}

run_stream_job() {
  # Roda o job de streaming ate drenar o backlog. Bloqueante (sai ao terminar).
  #   $1 = spark.executor.cores
  #   $2 = total-executor-cores (default = $1; cenario B passa workers*cores)
  # Submetido via `docker exec` no container do spark-master (fixado no infra).
  local exec_cores="$1"
  local total_cores="${2:-$1}"
  log "stream_job: executor.cores=${exec_cores}, total=${total_cores}, trigger='${TRIGGER}', topico=${TOPIC}, drenando backlog"
  local master_cid
  master_cid="$(wait_for_container "${SVC_MASTER}")"
  docker exec -i \
    -e SPARK_EXECUTOR_CORES="${exec_cores}" \
    -e SPARK_TOTAL_EXECUTOR_CORES="${total_cores}" \
    "${master_cid}" /app/spark-submit.sh \
      --topic "${TOPIC}" \
      --trigger "${TRIGGER}" \
      --watermark-delay "0 seconds" \
      --starting-offsets earliest \
      --await-timeout "${AWAIT_TIMEOUT}" \
      --idle-stop "${IDLE_STOP}" \
      --metrics-file "${METRICS_IN_CONTAINER}"
}

collect_run() {
  # Resume o JSONL deste run e anexa ao runs.csv. Tambem arquiva o JSONL.
  # O driver (no master, no infra) escreveu em REMOTE_DIR/output — lido aqui.
  local run_id="$1" label="$2"
  local archived="${METRICS_DIR}/${run_id}.jsonl"
  if [[ -f "${METRICS_ON_HOST}" ]]; then
    cp "${METRICS_ON_HOST}" "${archived}"
  else
    log "AVISO: metrics-file nao encontrado em ${METRICS_ON_HOST} (run ${run_id})"
    : > "${archived}"
  fi
  ${PY} "${HERE}/parse_metrics.py" run \
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

  # TOPICO UNICO POR RUN. Recriar um topico com o MESMO nome no KRaft single-broker
  # deixa offsets stale no broker (o `kafka-data` persiste metadados): o Spark
  # via "Found incorrect offsets ... some data may have been missed" e lia 0
  # linhas em runs subsequentes. Um nome novo a cada run e sempre pristino.
  TOPIC="${BASE_TOPIC}_${run_id}"
  export KAFKA_TOPIC="${TOPIC}"   # producer.py le o topico desta env

  log "===== RUN ${run_id} (topico=${TOPIC}) ====="
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
log "Iniciando benchmark SWARM | STACK='${STACK}' SCENARIOS='${SCENARIOS}' REPS=${REPS} MAX_RECORDS=${MAX_RECORDS}"
log "resultados em ${RESULTS_DIR}"

# A stack ja deve estar no ar (docker stack deploy -c docker-stack.yml ${STACK}).
# Confere que os servicos de infra existem antes de comecar (falha cedo se nao).
if ! docker service inspect "${SVC_KAFKA}" >/dev/null 2>&1; then
  log "ERRO: servico ${SVC_KAFKA} nao existe. Rode primeiro: docker stack deploy -c docker-stack.yml ${STACK}"
  exit 1
fi

for s in ${SCENARIOS}; do
  case "${s}" in
    A) scenario_A ;;
    B) scenario_B ;;
    *) log "cenario desconhecido: ${s} (ignorado)" ;;
  esac
done

# Agregacao final: runs.csv -> summary.csv (media + desvio por configuracao).
log "agregando resultados (media + desvio padrao)"
${PY} "${HERE}/parse_metrics.py" aggregate --runs-csv "${RUNS_CSV}" --out "${SUMMARY_CSV}"

log "PRONTO. runs em ${RUNS_CSV} | resumo em ${SUMMARY_CSV}"
