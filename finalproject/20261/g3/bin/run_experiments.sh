#!/usr/bin/env bash
# Harness de experimentos (>=3 repetições, média e desvio padrão).
# Cobre: W1 throughput por paralelismo, W3 EXACTLY_ONCE × AT_LEAST_ONCE,
#         W6 custo de checkpoint. (W4 recovery e W5 reconciliação têm scripts
#         dedicados: kill_taskmanager.sh + reconcile.sh; W7/W9 saem em collect.)
#
# É um DRIVER de referência: reinicia a stack a cada run para isolamento e usa
# um subconjunto controlado (MAX_RECORDS) para manter o tempo viável.
set -euo pipefail
 
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "$0")/.."

REPS="${REPS:-3}"
MAX_RECORDS="${MAX_RECORDS:-200000}"   # tamanho do workload por run
mkdir -p results

# Alvo determinístico de outcomes por run: total de ops do ledger sobre os
# primeiros MAX_RECORDS registros do input (independe da config). Usado para
# detectar o fim do dreno de forma robusta. Calculado uma única vez.
INPUT_CSV_HOST="$(grep -E '^INPUT_CSV=' .env 2>/dev/null | head -1 | cut -d= -f2-)"
INPUT_CSV_HOST="${INPUT_CSV_HOST:-./datasample/sample.csv.gz}"
ABS_IN="$(cd "$(dirname "$INPUT_CSV_HOST")" && pwd)/$(basename "$INPUT_CSV_HOST")"
case "$INPUT_CSV_HOST" in
  *.gz) zcat "$ABS_IN" | head -n $((MAX_RECORDS + 1)) > results/_expected_input.csv ;;
  *)    head -n $((MAX_RECORDS + 1)) "$ABS_IN" > results/_expected_input.csv ;;
esac
EXPECTED=$(docker compose run --rm --no-deps -v "$PWD/results":/r consumer python -c "
import sys; sys.path.insert(0, '/app/src')
from common.schema import iter_transactions, open_text
from common.ledger import expand_to_ops
n = 0
with open_text('/r/_expected_input.csv') as f:
    for tx in iter_transactions(f):
        n += len(expand_to_ops(tx))
print(n)
" 2>/dev/null | tr -d '[:space:]')
EXPECTED="${EXPECTED:-0}"
export EXPECTED
echo "[experiments] alvo de outcomes por run (MAX_RECORDS=$MAX_RECORDS): $EXPECTED"

# Executa um run isolado. $1=tag $2=config-string $3..=VAR=VAL extras.
run_once() {
  local tag="$1"; shift
  local config="$1"; shift
  echo ">>> [$tag] $config (reps=$REPS, max_records=$MAX_RECORDS)"
  for r in $(seq 1 "$REPS"); do
    echo "    rep $r/$REPS ..."
    docker compose down -v >/dev/null 2>&1 || true
    env "$@" RATE=0 MAX_RECORDS="$MAX_RECORDS" SETTLE_DRAIN_MS=5000 \
      docker compose up --build -d >/dev/null
    docker compose wait job-submitter >/dev/null 2>&1 || true
    local t0 t1
    t0=$(date +%s.%N)
    docker compose wait producer >/dev/null 2>&1 || true
    # Espera o settlement DRENAR por completo. Como o número de ops é
    # determinístico (EXPECTED, computado do baseline sobre os mesmos
    # MAX_RECORDS), esperamos a contagem de outcomes ATINGIR esse alvo. Isso é
    # robusto a pausas de checkpoint (o sink exactly-once só commita por
    # checkpoint) e à cauda do timer de drain, que um limiar de ociosidade
    # confundiria com "terminado". ``elapsed`` vai de t0 (início do producer) até
    # o instante em que o alvo foi atingido: vazão real ponta a ponta.
    local prev=0 cur last_change t_done=""
    last_change=$(date +%s.%N)
    for _ in $(seq 1 400); do
      cur=$(docker compose exec -T postgres psql -U pix -tA \
              -c 'select count(*) from outcomes;' 2>/dev/null | tr -d '[:space:]')
      [ -z "$cur" ] && cur=0
      if [ "$cur" -gt "$prev" ]; then prev=$cur; last_change=$(date +%s.%N); fi
      if [ "${EXPECTED:-0}" -gt 0 ] && [ "$cur" -ge "$EXPECTED" ]; then
        t_done=$(date +%s.%N); break
      fi
      sleep 3
    done
    t1="${t_done:-$last_change}"
    local elapsed
    elapsed=$(awk "BEGIN{print $t1-$t0}")
    echo "      outcomes=$prev/${EXPECTED:-?} elapsed=${elapsed}s"
    docker compose run --rm --no-deps -v "$PWD/results":/r consumer \
      python /app/src/experiments/collect.py \
        --elapsed "$elapsed" --tag "$tag" --config "$config" \
        --out /r/runs.csv \
      || true
  done
}

echo "=== W1: throughput por paralelismo ==="
for p in 1 2 4; do
  run_once "W1" "parallelism=$p" PARALLELISM="$p" GUARANTEE=EXACTLY_ONCE
done

echo "=== W3: EXACTLY_ONCE vs AT_LEAST_ONCE ==="
for g in EXACTLY_ONCE AT_LEAST_ONCE; do
  run_once "W3" "guarantee=$g" PARALLELISM=2 GUARANTEE="$g"
done

echo "=== W6: custo de checkpoint ==="
for c in 1000 5000 30000; do
  run_once "W6" "checkpoint_ms=$c" PARALLELISM=2 CHECKPOINT_MS="$c"
done

echo "=== Agregação e gráficos ==="
docker compose run --rm --no-deps -v "$PWD/results":/r consumer \
  python /app/src/experiments/aggregate.py --input /r/runs.csv --output /r/agg.csv
for m in tps lat_p95; do
  docker compose run --rm --no-deps -v "$PWD/results":/r consumer \
    python /app/src/experiments/plots.py --input /r/agg.csv --metric "$m" \
      --output "/r/plot_$m.png" --title "$m" || true
done

echo "[experiments] concluído. Veja results/runs.csv, results/agg.csv e results/plot_*.png"
