#!/usr/bin/env bash
# Gera o datasample (<=1MB) a partir do PaySim completo, dentro de um container.
# Uso: ./bin/make_sample.sh <paysim.csv> [saida.csv.gz]
set -euo pipefail
cd "$(dirname "$0")/.."

INPUT="${1:?uso: make_sample.sh <paysim.csv> [saida.csv.gz]}"
OUTPUT="${2:-datasample/sample.csv.gz}"
OUTDIR="$(cd "$(dirname "$OUTPUT")" && pwd)"

docker build -t pix-app:local -f misc/app/Dockerfile .
docker run --rm \
  -v "$(cd "$(dirname "$INPUT")" && pwd)/$(basename "$INPUT")":/data/in.csv:ro \
  -v "$OUTDIR":/out \
  pix-app:local \
  python /app/src/data/make_sample.py --input /data/in.csv \
         --output "/out/$(basename "$OUTPUT")"
echo "[make_sample] gerado: $OUTPUT"
