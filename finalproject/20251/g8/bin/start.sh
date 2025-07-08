#!/bin/bash

# Nome do diretório raiz do seu grupo (ex: g1, g2, etc.)
# Obtém o diretório pai do script (que deve ser 'bin') e então o diretório pai deste (gX)
PROJECT_ROOT=$(dirname "$(dirname "$(readlink -f "$0")")")

echo "--- Iniciando o Ambiente Spark e JupyterLab ---"

# 1. Verificar a existência da pasta de dados completos
# echo "Verificando a pasta de dados completos: ${PROJECT_ROOT}/full_data"
# if [ ! -d "${PROJECT_ROOT}/full_data" ]; then
#     echo "AVISO: A pasta '${PROJECT_ROOT}/full_data' não foi encontrada."
#     echo "Por favor, garanta que seus arquivos CSV completos estejam lá, conforme o README.md."
#     echo "O Spark e JupyterLab podem não ter acesso aos dados completos sem ela."
#     # Opcional: Você pode adicionar um 'exit 1' aqui para forçar o usuário a criar a pasta
#     # exit 1
# fi

# 2. Navegar para o diretório raiz do projeto para executar o docker-compose
echo "Navegando para o diretório raiz do projeto: ${PROJECT_ROOT}"
cd "${PROJECT_ROOT}" || { echo "Erro: Não foi possível navegar para ${PROJECT_ROOT}"; exit 1; }

# 3. Iniciar o Docker Compose
echo "Executando 'docker compose up --build -d'..."
docker-compose up --build -d

# Verificar se o docker-compose foi bem-sucedido
if [ $? -eq 0 ]; then
    echo "Ambiente Docker iniciado com sucesso!"
    echo "--------------------------------------------------------"
    echo "Você pode acessar o JupyterLab no seu navegador:"
    echo "http://localhost:8888"
    echo "--------------------------------------------------------"
    echo "Para submeter um job Spark (ex: seu_script.py), use:"
    echo "docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/bitnami/spark/src/process_csv.py"
    echo "Para parar o ambiente, execute './bin/stop.sh' (se você criar um)."
    echo "Ou manualmente, navegue para ${PROJECT_ROOT} e execute 'docker-compose down'."
else
    echo "Erro ao iniciar o ambiente Docker. Por favor, verifique as mensagens acima."
fi