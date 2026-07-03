#!/usr/bin/env bash
# =========================================================================== #
# Copia os artefatos necessarios para o node-infra (AWS).
#
# No Swarm multi-host cada VM tem disco proprio. So o node-infra precisa dos
# arquivos: e la que rodam o Spark master (driver/spark-submit), o producer (no
# host) e a escrita de output/checkpoints. Os spark-workers (node-w1..3) NAO
# recebem nada — leem do Kafka e recebem as tasks do driver; o conector Kafka
# vem via --packages no spark-submit.
#
# O que e copiado para  ${REMOTE_DIR} (default /home/ubuntu/taxi) no node-infra:
#   spark/         codigo PySpark  (montado em /app        no spark-master)
#   zones/         taxi_zone_lookup.csv (montado em /zones no spark-master)
#   data_subset/   ~1.1 GB de parquet — lido pelo PRODUCER (roda no host infra)
#   benchmarks/    harness de benchmark (run/parse/plot) — roda no host infra
#   producer/      producer.py + requirements.txt — roda no host infra
#   .env           variaveis da stack (lido pelo docker stack deploy)
#   docker-stack.yml  a stack a ser deployada
# Cria tambem output/ e checkpoints/ vazios no destino (bind-mounts rw).
#
# data_subset/ contem SYMLINKS para ../data/*.parquet; rsync -L / scp seguem os
# links e copiam os arquivos reais.
#
# Uso:
#   ./distribute_files.sh                 # usa os defaults abaixo
#   INFRA_IP=1.2.3.4 ./distribute_files.sh
# =========================================================================== #
set -euo pipefail

# --- Config (sobrescrevivel por env) --------------------------------------- #
INFRA_IP="${INFRA_IP:-100.54.219.116}"                 # IP PUBLICO do node-infra (SSH)
SSH_KEY="${SSH_KEY:-$HOME/.ssh/taxi-swarm-key.pem}"
SSH_USER="${SSH_USER:-ubuntu}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/taxi}"           # deve casar com os binds do docker-stack.yml
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"   # raiz g11/ (fontes espalhadas em src/, misc/, datasample/)

SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=accept-new)
REMOTE="${SSH_USER}@${INFRA_IP}"

echo "[distribute] destino: ${REMOTE}:${REMOTE_DIR}"

# --- 1) Estrutura de diretorios no destino --------------------------------- #
ssh "${SSH_OPTS[@]}" "$REMOTE" "mkdir -p \
  '${REMOTE_DIR}/spark' \
  '${REMOTE_DIR}/zones' \
  '${REMOTE_DIR}/data_subset' \
  '${REMOTE_DIR}/benchmarks' \
  '${REMOTE_DIR}/producer' \
  '${REMOTE_DIR}/output' \
  '${REMOTE_DIR}/checkpoints'"

# --- 2) Copia dos artefatos ------------------------------------------------ #
# Prefere rsync (incremental, segue symlinks com -L); cai para scp se ausente.
# Fontes na estrutura g11/: codigo em src/, config em misc/. DATA_SRC e o
# dataset a enviar para o experimento AWS: por padrao a amostra em datasample/,
# mas para o experimento real aponte para o dataset completo:
#     DATA_SRC=../data ./bin/distribute_files.sh   (relativo a g11/)
DATA_SRC="${DATA_SRC:-${ROOT}/datasample}"
if command -v rsync >/dev/null 2>&1; then
  RSYNC=(rsync -avL --delete -e "ssh ${SSH_OPTS[*]}")
  echo "[distribute] usando rsync (-L segue eventuais symlinks)"
  "${RSYNC[@]}" "${ROOT}/src/spark/"       "${REMOTE}:${REMOTE_DIR}/spark/"
  "${RSYNC[@]}" "${ROOT}/misc/zones/"      "${REMOTE}:${REMOTE_DIR}/zones/"
  "${RSYNC[@]}" "${DATA_SRC}/"             "${REMOTE}:${REMOTE_DIR}/data_subset/"
  "${RSYNC[@]}" "${ROOT}/src/benchmarks/"  "${REMOTE}:${REMOTE_DIR}/benchmarks/"
  "${RSYNC[@]}" "${ROOT}/src/producer/"    "${REMOTE}:${REMOTE_DIR}/producer/"
else
  echo "[distribute] rsync ausente — usando scp (-r segue symlinks)"
  scp "${SSH_OPTS[@]}" -r "${ROOT}/src/spark/."      "${REMOTE}:${REMOTE_DIR}/spark/"
  scp "${SSH_OPTS[@]}" -r "${ROOT}/misc/zones/."     "${REMOTE}:${REMOTE_DIR}/zones/"
  scp "${SSH_OPTS[@]}" -r "${DATA_SRC}/."            "${REMOTE}:${REMOTE_DIR}/data_subset/"
  scp "${SSH_OPTS[@]}" -r "${ROOT}/src/benchmarks/." "${REMOTE}:${REMOTE_DIR}/benchmarks/"
  scp "${SSH_OPTS[@]}" -r "${ROOT}/src/producer/."   "${REMOTE}:${REMOTE_DIR}/producer/"
fi

# --- 3) Arquivos avulsos (.env + a propria stack) -------------------------- #
scp "${SSH_OPTS[@]}" "${ROOT}/misc/.env"             "${REMOTE}:${REMOTE_DIR}/.env"
scp "${SSH_OPTS[@]}" "${ROOT}/misc/docker-stack.yml" "${REMOTE}:${REMOTE_DIR}/docker-stack.yml"

echo "[distribute] concluido. Conteudo em ${REMOTE}:${REMOTE_DIR}:"
ssh "${SSH_OPTS[@]}" "$REMOTE" "ls -la '${REMOTE_DIR}' && echo '--- data_subset (arquivos reais) ---' && du -sh '${REMOTE_DIR}/data_subset'"
