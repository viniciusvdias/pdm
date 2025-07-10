#!/bin/bash

# Build and Run ENEM Spark Pipeline
# This script builds the Docker image and runs the complete pipeline

set -e

echo "Escolha o tipo de dados para processar:"
echo "1) Dados locais"
echo "2) Dados reais do ENEM"
echo ""
read -p "Digite sua escolha (1 ou 2): " choice

case $choice in
    1)
        echo ""
        echo "Usando dados locais (amostra pequena)"
        export USE_LOCAL_DATA=true
        ;;
    2)
        echo ""
        echo "Usando dados reais do ENEM"
        echo ""
        read -p "Tem certeza? (y/N): " confirm
        if [[ $confirm =~ ^[Yy]$ ]]; then
            export USE_LOCAL_DATA=false
        else
            echo "Operação cancelada."
            exit 0
        fi
        ;;
    *)
        echo "❌ Escolha inválida. Use 1 ou 2."
        exit 1
        ;;
esac

echo ""
echo "Configuração de escala do cluster:"
echo "1) Padrão (2 workers Spark, 1 datanode)"
echo "2) Configuração customizada"
echo ""
read -p "Digite sua escolha (1 ou 2): " scale_choice

case $scale_choice in
    1)
        SPARK_WORKERS=2
        DATANODES=1
        echo "Usando configuração padrão: 2 workers Spark, 1 datanode"
        ;;
    2)
        echo ""
        read -p "Número de workers Spark (1-4): " workers_input
        read -p "Número de datanodes (1-3): " datanodes_input
        
        # Validate inputs
        if ! [[ "$workers_input" =~ ^[1-4]$ ]]; then
            echo "❌ Número de workers deve ser entre 1 e 4. Usando padrão (2)."
            SPARK_WORKERS=2
        else
            SPARK_WORKERS=$workers_input
        fi
        
        if ! [[ "$datanodes_input" =~ ^[1-3]$ ]]; then
            echo "❌ Número de datanodes deve ser entre 1 e 3. Usando padrão (1)."
            DATANODES=1
        else
            DATANODES=$datanodes_input
        fi
        
        echo "Configuração: $SPARK_WORKERS workers Spark, $DATANODES datanodes"
        ;;
    *)
        echo "❌ Escolha inválida. Usando configuração padrão."
        SPARK_WORKERS=2
        DATANODES=1
        ;;
esac

echo ""
echo "Building Docker image..."
docker build -t enem-spark-job -f misc/Dockerfile .

echo ""
echo "Starting services with Docker Compose..."
cd misc
docker-compose up --scale spark-worker=$SPARK_WORKERS --scale datanode=$DATANODES -d

echo ""
echo "Pipeline started successfully!"
echo "Access Spark Master UI at: http://localhost:8080"
echo "Access HDFS UI at: http://localhost:9870"
echo ""
if [ "$USE_LOCAL_DATA" = "true" ]; then
    echo "Processando dados locais (rápido)..."
else
    echo "Processando dados reais (pode demorar várias horas)..."
fi
echo ""
echo "To stop the services, run: ./bin/stop.sh"
