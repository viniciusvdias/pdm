#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Obtem o dataset COMPLETO (Dogs vs. Cats, Kaggle) e o descompacta em output/data/raw_jpg/.
#
# O Kaggle exige login, entao o download do zip e MANUAL:
#   1. Baixe "train.zip" em: https://www.kaggle.com/c/dogs-vs-cats/data
#   2. Coloque o arquivo em:  gX/output/data/raw_jpg/train.zip
#   3. Rode este script: ./bin/download_data.sh   (descompacta usando o proprio conteiner)
#
# Alternativa (Kaggle CLI): se tiver o ~/.kaggle/kaggle.json configurado, descomente o bloco abaixo.
# -----------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RAW="output/data/raw_jpg"
mkdir -p "$RAW"
COMPOSE="docker compose -f misc/docker-compose.yml"

# --- Opcional: download automatico via Kaggle CLI (requer credencial) -------
# kaggle competitions download -c dogs-vs-cats -f train.zip -p "$RAW"
# ----------------------------------------------------------------------------

if [ ! -f "$RAW/train.zip" ]; then
  echo "[!] $RAW/train.zip nao encontrado."
  echo "    Baixe manualmente de https://www.kaggle.com/c/dogs-vs-cats/data e coloque ali."
  exit 1
fi

echo ">>> descompactando train.zip (dentro do conteiner, so Docker necessario)"
$COMPOSE build
$COMPOSE run --rm bigdata python -c \
  "import zipfile; zipfile.ZipFile('/tf/output/data/raw_jpg/train.zip').extractall('/tf/output/data/raw_jpg'); print('ok')"

echo ">>> pronto. As imagens serao movidas/duplicadas pelo bin/run_full.sh (etapa 01)."
