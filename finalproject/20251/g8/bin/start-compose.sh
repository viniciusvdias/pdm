#!/bin/bash
set -e

# Define o DATAMODE com base no primeiro argumento do script ($1).
# Se nenhum argumento for passado, usa 'datasample' como padrão.
DATAMODE=${1:-datasample}

# Valida se o argumento é um dos valores esperados
if [ "$DATAMODE" != "datasample" ] && [ "$DATAMODE" != "full_data" ]; then
  echo "❌ Erro: Parâmetro inválido. Use 'datasample' ou 'full_data'."
  exit 1
fi

echo "➡️  Configurando ambiente para rodar com: $DATAMODE"

# Cria (ou sobrescreve) o arquivo .env para o Docker Compose usar
echo "DATAMODE=$DATAMODE" > .env

echo "➡️  Construindo e iniciando o ambiente com Docker Compose..."

# Usando 'docker compose' em vez de 'docker-compose' (padrão mais novo)
docker compose up --build -d

echo "✅  Ambiente Compose iniciado com sucesso no modo '$DATAMODE'!"
echo "   - Spark Master UI: http://localhost:8080"
echo "   - JupyterLab: http://localhost:8888"