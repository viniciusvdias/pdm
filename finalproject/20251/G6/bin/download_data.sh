#!/usr/bin/env bash

if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <data_inicio YYYY-MM> <data_fim YYYY-MM>"
    exit 1
fi

# Converte as datas de entrada para segundos desde a época para comparação
start_date=$(date -d "$1-01" +%s)
end_date=$(date -d "$2-01" +%s)

# Itera do ano e mês de início até o fim
current_date=$start_date
while [ "$current_date" -le "$end_date" ]; do
    year=$(date -d @$current_date +%Y)
    month=$(date -d @$current_date +%m)

    # Monta a URL e o nome do arquivo
    url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet"
    file_name="yellow_tripdata_${year}-${month}.parquet"

    # Faz o download do arquivo
    echo "Baixando o arquivo: $file_name"
    wget -O "../data/$file_name" "$url"

    # Verifica se o download foi bem-sucedido
    if [ $? -ne 0 ]; then
        echo "Falha ao baixar $file_name. Verifique se o arquivo existe para esta data."
        # Remove o arquivo vazio em caso de falha
        rm -f "../data/$file_name"
    fi

    # Avança para o próximo mês
    current_date=$(date -d "$(date -d @$current_date +%Y-%m-%d) +1 month" +%s)
done

echo "Download concluído!"
