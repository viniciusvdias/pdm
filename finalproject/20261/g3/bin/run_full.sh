#!/usr/bin/env bash
# Roda a stack com o dataset completo (amplificado).
# Uso: ./bin/run_full.sh <caminho_do_csv_completo_ou_amplificado>
set -euo pipefail
 
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "$0")/.."

FULL="${1:?uso: run_full.sh <csv_completo>}"
ABS="$(cd "$(dirname "$FULL")" && pwd)/$(basename "$FULL")"

[ -f .env ] || cp .env.example .env
# Atualiza/insere INPUT_CSV no .env.
if grep -q '^INPUT_CSV=' .env; then
  sed -i.bak "s|^INPUT_CSV=.*|INPUT_CSV=$ABS|" .env && rm -f .env.bak
else
  echo "INPUT_CSV=$ABS" >> .env
fi
echo "[run_full] INPUT_CSV=$ABS"
exec ./bin/run.sh
