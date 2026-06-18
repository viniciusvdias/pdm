# Pipeline de Streaming BTC/USDT — Binance → Kafka → Spark

Pipeline de dados em tempo real que coleta trades de BTC/USDT da Binance, processa com Spark Structured Streaming e gera métricas de desempenho para análise.

## Arquitetura

```
Binance WebSocket → binance.py (Producer) → Kafka → processor.py (Spark) → CSVs de métricas
                                                                          → Indicadores (EMA, RSI)
                                                                          → analysis.ipynb (gráficos)
```

## Pré-requisitos

- **Docker Desktop** (com WSL integration habilitado)
- **WSL** (Ubuntu)
- **Python 3.12** (dentro do WSL)
- **Java 11+** (dentro do WSL — necessário para o Spark)

### Instalação dos pré-requisitos no WSL

```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip default-jdk
```

Verificar:

```bash
java -version
python3 --version
docker --version
```

## Setup do projeto

```bash
cd /mnt/c/Users/<seu-usuario>/caminho/para/projeto-big-data

# Criar e instalar dependências no venv
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

## Como rodar

### 1. Subir o Kafka

```bash
docker compose up -d
```

Verificar se está rodando:

```bash
docker ps
```

### 2. Criar o tópico

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic trades --partitions 1 --replication-factor 1
```

### 3. Iniciar o coletor (Terminal 1)

```bash
venv/bin/python -u binance.py
```

Deve imprimir: `Conectado a wss://stream.binance.com:9443/stream?streams=btcusdt@trade`

### 4. Iniciar o processador (Terminal 2)

```bash
EXPERIMENT_LABEL=p1_w10 WINDOW_SIZE_SECONDS=10 PARTITIONS=1 venv/bin/python -u processor.py
```

Após ~15 segundos os candles OHLCV começam a aparecer. Após 14 candles, os indicadores EMA(9) e RSI(14) são calculados.

Os dados são salvos automaticamente em:
- `metrics/performance_p1_w10.csv` — métricas de desempenho (latência, throughput)
- `metrics/candles_p1_w10.csv` — candles com indicadores

### 5. Parar

`Ctrl+C` em ambos os terminais.

## Experimentos de desempenho

Para comparar diferentes configurações, rode múltiplos experimentos variando partições e tamanho de janela.

### Exemplo: comparar 1 vs 4 partições

Após parar o pipeline anterior:

```bash
# Deletar e recriar tópico com 4 partições
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic trades
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic trades --partitions 4 --replication-factor 1

# Terminal 1
venv/bin/python -u binance.py

# Terminal 2
EXPERIMENT_LABEL=p4_w10 WINDOW_SIZE_SECONDS=10 PARTITIONS=4 venv/bin/python -u processor.py
```

### Exemplo: variar tamanho de janela

```bash
# Janela de 5 segundos
EXPERIMENT_LABEL=p1_w5 WINDOW_SIZE_SECONDS=5 PARTITIONS=1 venv/bin/python -u processor.py

# Janela de 30 segundos
EXPERIMENT_LABEL=p1_w30 WINDOW_SIZE_SECONDS=30 PARTITIONS=1 venv/bin/python -u processor.py
```

### Automação (Linux/WSL)

O script `experiment.sh` automatiza todas as rodadas:

```bash
chmod +x experiment.sh

# Rodar todos os experimentos (5 configs × 120s cada)
./experiment.sh

# Ou com duração menor por rodada
DURATION=60 ./experiment.sh
```

## Análise dos resultados

Após rodar os experimentos, abra o notebook:

```bash
venv/bin/jupyter notebook analysis.ipynb
```

O notebook gera automaticamente:
- Throughput por número de partições e tamanho de janela
- Distribuição de latência (p50, p95, p99)
- Latência ao longo do tempo
- Gráfico de candles com EMA(9) e RSI(14)
- Tabela resumo para o relatório

## Variáveis de ambiente

| Variável | Default | Descrição |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Endereço do broker Kafka |
| `KAFKA_TRADES_TOPIC` | `trades` | Nome do tópico |
| `BINANCE_SYMBOLS` | `BTCUSDT` | Pares para coletar (separados por vírgula) |
| `WINDOW_SIZE_SECONDS` | `10` | Tamanho da janela de agregação |
| `WATERMARK_SECONDS` | `10` | Tolerância para dados atrasados |
| `TRIGGER_SECONDS` | `5` | Intervalo de processamento do Spark |
| `EXPERIMENT_LABEL` | `default` | Label para nomear os CSVs de saída |
| `METRICS_DIR` | `metrics` | Diretório dos CSVs de métricas |

## Estrutura do projeto

```
├── binance.py          # Coletor: Binance WebSocket → Kafka
├── consumer.py         # Consumidor simples (debug)
├── processor.py        # Processador Spark: Kafka → OHLCV + indicadores + métricas
├── config.py           # Configuração centralizada via .env
├── experiment.sh       # Automação dos experimentos
├── analysis.ipynb      # Notebook de análise e gráficos
├── docker-compose.yml  # Kafka (KRaft)
├── run.sh              # Script de orquestração (Linux)
├── requirements.txt    # Dependências Python
└── metrics/            # CSVs gerados pelos experimentos
```

## Desligar o Kafka

```bash
docker compose down
```
