#!/bin/bash

# Nome do diretório raiz do seu grupo
PROJECT_ROOT=$(dirname "$(dirname "$(readlink -f "$0")")")
FULL_DATA_DIR="${PROJECT_ROOT}/full_data"
DATASAMPLE_DIR="${PROJECT_ROOT}/datasample"

echo "--- Gerando Amostra de Dados ---"

# Verificar se a pasta datasample existe, senão criar
if [ ! -d "$DATASAMPLE_DIR" ]; then
    echo "Criando diretório de amostra de dados: ${DATASAMPLE_DIR}"
    mkdir -p "$DATASAMPLE_DIR"
fi

# Solicitar o nome do arquivo CSV grande
read -p "Por favor, digite o nome do arquivo CSV grande (ex: meu_arquivo_grande.csv) que está em '${FULL_DATA_DIR}': " LARGE_CSV_FILENAME

SOURCE_CSV="${FULL_DATA_DIR}/${LARGE_CSV_FILENAME}"
SAMPLE_CSV="${DATASAMPLE_DIR}/sample_${LARGE_CSV_FILENAME}" # Ex: sample_meu_arquivo_grande.csv

# Verificar se o arquivo CSV grande existe
if [ ! -f "$SOURCE_CSV" ]; then
    echo "Erro: Arquivo '${SOURCE_CSV}' não encontrado."
    echo "Certifique-se de que o arquivo CSV grande existe e está na pasta '${FULL_DATA_DIR}'."
    exit 1
fi

echo "Gerando amostra de 100 linhas (com cabeçalho) de '${SOURCE_CSV}' para '${SAMPLE_CSV}'..."

# Pega o cabeçalho (primeira linha)
head -n 1 "$SOURCE_CSV" > "$SAMPLE_CSV"

# Pega as próximas 100 linhas (a partir da segunda linha)
tail -n +2 "$SOURCE_CSV" | head -n 100 >> "$SAMPLE_CSV"

if [ $? -eq 0 ]; then
    echo "Amostra gerada com sucesso! Verifique em '${SAMPLE_CSV}'."
    echo "Total de linhas na amostra (incluindo cabeçalho): $(wc -l < "$SAMPLE_CSV")"
else
    echo "Erro ao gerar a amostra de dados."
fi