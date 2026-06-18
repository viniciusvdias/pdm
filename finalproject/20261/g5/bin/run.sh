#!/bin/bash
# Sobe o projeto completo: Neo4j → injector → API.
# Uso: ./bin/run.sh [--full-data]
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

FULL_DATA=0
if [[ "$1" == "--full-data" ]]; then
  FULL_DATA=1
fi

echo "=== Sistema de Recomendacao de Filmes - Grupo 5 ==="
echo ""

if [[ "$FULL_DATA" == "1" ]]; then
  if [ ! -d "./data" ] || [ -z "$(ls -A ./data 2>/dev/null)" ]; then
    echo "Dataset completo nao encontrado. Baixando do Google Drive..."
    bash bin/download-dataset.sh
  fi
  # Remonta o injector apontando para ./data em vez de ./datasample
  export COMPOSE_FILE=docker-compose.yml
  docker compose run --rm \
    -v "$SCRIPT_DIR/data:/data" \
    injector-job
  docker compose up -d api-gateway
else
  echo "Usando datasample (modo rapido)."
  docker compose up --build -d
fi

echo ""
echo "Aguardando API ficar pronta..."
until docker compose exec api-gateway wget -q --spider http://localhost:3000/health 2>/dev/null; do
  printf "."
  sleep 2
done

echo ""
echo "=== Tudo pronto! ==="
echo "  Neo4j Browser : http://localhost:7474  (usuario: neo4j / senha: g5password)"
echo "  API            : http://localhost:3000"
echo "  Swagger UI     : http://localhost:3000/docs"
echo ""
echo "Exemplos:"
echo "  curl 'http://localhost:3000/health'"
echo "  curl 'http://localhost:3000/recommend?userId=1'"
echo "  curl 'http://localhost:3000/recommend/collaborative?userId=1'"
echo "  curl 'http://localhost:3000/recommend/tag-genome?userId=1'"
