#!/bin/bash
# Baixa o dataset genome_2021 do Google Drive usando gdown (via Docker).
# Requer apenas Docker instalado.
set -e

DEST="$(cd "$(dirname "$0")/.." && pwd)/data"
FOLDER_ID="11jNkzP_r_5cdXkGDP2aLYynZwgcZcFDx"

mkdir -p "$DEST"

echo "Baixando genome_2021 do Google Drive para $DEST ..."
docker run --rm \
  -v "$DEST:/data" \
  python:3.12-slim \
  sh -c "pip install -q gdown && gdown --folder '$FOLDER_ID' -O /data --remaining-ok"

echo ""
echo "Download concluido. Arquivos em $DEST:"
ls -lh "$DEST"
echo ""
echo "Para injetar o dataset completo no Neo4j, monte o volume:"
echo "  docker compose run -v $DEST:/data injector-job"
