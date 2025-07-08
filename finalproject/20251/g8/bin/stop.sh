#!/bin/bash

# Nome do diretório raiz do seu grupo
PROJECT_ROOT=$(dirname "$(dirname "$(readlink -f "$0")")")

echo "--- Parando o Ambiente Spark e JupyterLab ---"

# Navegar para o diretório raiz do projeto para executar o docker-compose
echo "Navegando para o diretório raiz do projeto: ${PROJECT_ROOT}"
cd "${PROJECT_ROOT}" || { echo "Erro: Não foi possível navegar para ${PROJECT_ROOT}"; exit 1; }

# Parar e remover os contêineres e redes criadas pelo docker-compose
echo "Executando 'docker-compose down'..."
docker compose down

if [ $? -eq 0 ]; then
    echo "Ambiente Docker parado e recursos liberados com sucesso."
else
    echo "Erro ao parar o ambiente Docker. Por favor, verifique as mensagens acima."
fi