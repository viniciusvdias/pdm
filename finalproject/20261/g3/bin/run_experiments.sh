#!/usr/bin/env bash
# Harness de experimentos (>=3 repetições, média e desvio padrão).
# Cobre: W1 throughput por paralelismo, W3 EXACTLY_ONCE × AT_LEAST_ONCE,
#         W6 custo de checkpoint. (W4 recovery e W5 reconciliação têm scripts
#         dedicados: kill_taskmanager.sh + reconcile.sh; W7/W9 saem em collect.)
#
# É um DRIVER de referência: reinicia a stack a cada run para isolamento e usa
# um subconjunto controlado (MAX_RECORDS) para manter o tempo viável.
set -euo pipefail
cd "$(dirname "$0")/.."

REPS="${REPS:-3}"
MAX_RECORDS="${MAX_RECORDS:-200000}"   # tamanho do workload por run
mkdir -p results

# Executa um run isolado. $1=tag $2=config-string $3..=VAR=VAL extras.
run_once() {
  local tag="$1"; shift
  local config="$1"; shift
  echo ">>> [$tag] $config (reps=$REPS, max_records=$MAX_RECORDS)"
  for r in $(seq 1 "$REPS"); do
    echo "    rep $r/$REPS ..."
    docker compose down -v >/dev/null 2>&1 || true
    env "$@" RATE=0 MAX_RECORDS="$MAX_RECORDS" \
      docker compose up --build -d >/dev/null
    docker compose wait job-submitter >/dev/null 2>&1 || true
    local t0 t1
    t0=$(date +%s.%N)
    docker compose wait producer >/dev/null 2>&1 || true
    sleep 10   # deixa o consumer drenar os outcomes finais
    t1=$(date +%s.%N)
    local elapsed
    elapsed=$(awk "BEGIN{print $t1-$t0}")
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
