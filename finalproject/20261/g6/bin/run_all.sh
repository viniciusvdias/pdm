#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate

: "${GRAPH_RAW_PATH:=data/raw/soc-orkut-relationships.txt}"

bash bin/download_dataset.sh
python -m cli.main benchmark --input "$GRAPH_RAW_PATH" --fractions 100 --runs 3
python -m cli.main report
