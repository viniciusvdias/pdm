#!/usr/bin/env bash
# Gera presentation/presentation.pdf de forma reprodutível (container app).
set -euo pipefail
cd "$(dirname "$0")/.."

docker build -t pix-app:local -f misc/app/Dockerfile .
docker run --rm -v "$PWD/presentation":/out pix-app:local \
  python /app/src/presentation/make_slides.py --output /out/presentation.pdf
echo "[make_slides] gerado: presentation/presentation.pdf"
