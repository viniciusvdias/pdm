#!/usr/bin/env bash
# Amplifica o PaySim para >=1GB, dentro de um container.
# Uso: ./bin/amplify.sh <paysim.csv> <saida.csv> [factor]
set -euo pipefail
cd "$(dirname "$0")/.."

INPUT="${1:?uso: amplify.sh <paysim.csv> <saida.csv> [factor]}"
OUTPUT="${2:?uso: amplify.sh <paysim.csv> <saida.csv> [factor]}"
FACTOR="${3:-3}"
OUTDIR="$(cd "$(dirname "$OUTPUT")" && pwd)"

docker build -t pix-app:local -f misc/app/Dockerfile .
docker run --rm \
  -v "$(cd "$(dirname "$INPUT")" && pwd)/$(basename "$INPUT")":/data/in.csv:ro \
  -v "$OUTDIR":/out \
  pix-app:local \
  python /app/src/data/amplify.py --input /data/in.csv \
         --output "/out/$(basename "$OUTPUT")" --factor "$FACTOR"
echo "[amplify] gerado (×$FACTOR): $OUTPUT"
echo "[amplify] aponte INPUT_CSV=$OUTPUT no .env e rode ./bin/run.sh"
