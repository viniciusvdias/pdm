@echo off
setlocal enabledelayedexpansion
REM Build and Run ENEM Spark Pipeline
REM This script builds the Docker image and runs the complete pipeline

echo ENEM Big Data Pipeline
echo =========================
echo.
echo Escolha o tipo de dados para processar:
echo 1^) Dados locais ^(rapido, apenas amostra de 15 registros^)
echo 2^) Dados reais do ENEM ^(completo, ~13M registros, demora varias horas^)
echo.
set /p choice="Digite sua escolha (1 ou 2): "

if "%choice%"=="1" (
    echo.
    echo ✅ Usando dados locais ^(amostra pequena^)
    echo 📊 Processando 15 registros de exemplo...
    set USE_LOCAL_DATA=true
) else if "%choice%"=="2" (
    echo.
    echo ⚠️  Usando dados reais do ENEM
    echo 📊 Processando ~13 milhoes de registros...
    echo ⏰ Tempo estimado: 6-8 horas
    echo.
    set /p confirm="Tem certeza? (y/N): "
    if /i "!confirm!"=="y" (
        set USE_LOCAL_DATA=false
    ) else (
        echo Operacao cancelada.
        exit /b 0
    )
) else (
    echo Escolha invalida. Use 1 ou 2.
    exit /b 1
)

echo.
echo Configuracao de escala do cluster:
echo 1^) Padrao ^(2 workers Spark, 1 datanode^)
echo 2^) Configuracao customizada
echo.
set /p scale_choice="Digite sua escolha (1 ou 2): "

if "%scale_choice%"=="1" (
    set SPARK_WORKERS=2
    set DATANODES=1
    echo Usando configuracao padrao: 2 workers Spark, 1 datanode
) else if "%scale_choice%"=="2" (
    echo.
    set /p workers_input="Numero de workers Spark (1-4): "
    set /p datanodes_input="Numero de datanodes (1-3): "
    
    REM Validate workers input
    if "%workers_input%"=="1" (
        set SPARK_WORKERS=1
    ) else if "%workers_input%"=="2" (
        set SPARK_WORKERS=2
    ) else if "%workers_input%"=="3" (
        set SPARK_WORKERS=3
    ) else if "%workers_input%"=="4" (
        set SPARK_WORKERS=4
    ) else (
        echo ❌ Numero de workers deve ser entre 1 e 4. Usando padrao ^(2^).
        set SPARK_WORKERS=2
    )
    
    REM Validate datanodes input
    if "%datanodes_input%"=="1" (
        set DATANODES=1
    ) else if "%datanodes_input%"=="2" (
        set DATANODES=2
    ) else if "%datanodes_input%"=="3" (
        set DATANODES=3
    ) else (
        echo ❌ Numero de datanodes deve ser entre 1 e 3. Usando padrao ^(1^).
        set DATANODES=1
    )
    
    echo Configuracao: !SPARK_WORKERS! workers Spark, !DATANODES! datanodes
) else (
    echo ❌ Escolha invalida. Usando configuracao padrao.
    set SPARK_WORKERS=2
    set DATANODES=1
)

echo.
echo Building Docker image...
docker build -t enem-spark-job -f misc/Dockerfile .

echo.
echo Starting services with Docker Compose...
cd misc
docker-compose up --scale spark-worker=%SPARK_WORKERS% --scale datanode=%DATANODES% -d

echo.
echo Pipeline started successfully!
echo Access Spark Master UI at: http://localhost:8080
echo Access HDFS UI at: http://localhost:9870
echo.
if "%USE_LOCAL_DATA%"=="true" (
    echo 📈 Processando dados locais ^(rapido^)...
) else (
    echo 📈 Processando dados reais ^(pode demorar varias horas^)...
)
echo.
echo To stop the services, run: bin\stop.bat
