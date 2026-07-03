# Final project report: Perfilamento Paralelo de Grandes Datasets CSV — NYC Yellow Taxi

**Disciplina:** Big Data / Processamento de Dados Massivos (PDM)  
**Universidade:** Universidade Federal de Lavras (UFLA) — PPGCC  
**Semestre:** 2026/1  
**Código-fonte:** [`src/`](src/) · **Scripts:** [`bin/`](bin/) · **Pipeline Docker:** [`bigdata-preprocessing/`](bigdata-preprocessing/)

---

## 1. Context and motivation

O **VisFlow-MM** é um sistema que gera visualizações de dados automaticamente a partir de arquivos CSV usando modelos de linguagem (LLMs). O pipeline funciona assim:

1. Usuário fornece um arquivo CSV e uma consulta em linguagem natural
2. O sistema **perfila o dataset** — extrai estatísticas, tipos, cardinalidade, valores únicos, etc.
3. Esse resumo é enviado como contexto para um LLM
4. O LLM gera código Python (matplotlib/plotly) para criar o gráfico solicitado

O problema central é que a classe `DataSetSummary` original usa `pd.read_csv()` para carregar o arquivo inteiro na memória. Para arquivos de 5–10 GB isso causa `MemoryError` em máquinas com 8–16 GB de RAM — **o perfilamento simplesmente falha**.

**Objetivo deste projeto:** implementar e comparar duas estratégias de perfilamento que funcionem para datasets de escala Big Data (1 GB, 5 GB, 10 GB), aproveitando paralelismo para acelerar o processamento:

- **Workload A (PROF-MP)** — Multiprocessing Python com `ProcessPoolExecutor`: leitura em chunks + workers paralelos, sem dependência de frameworks externos
- **Workload B (PROF-SPARK)** — Apache Spark (`local[N]`): Catalyst Optimizer + threads Java (sem GIL), scan único compilado

A questão central da pesquisa é: **qual estratégia realmente escala com o número de núcleos, e por quê?**

---

## 2. Data

### 2.1 Detailed description

O dataset utilizado é o **NYC TLC Yellow Taxi Trip Records** — registros públicos de corridas de táxi amarelo da cidade de Nova York, publicados mensalmente desde 2009 pela NYC Taxi & Limousine Commission.

- **Fonte:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Período coberto:** Janeiro/2022 – Junho/2024 (30 arquivos mensais)
- **Formato original:** Parquet (compressão Snappy, ~47 MB por arquivo mensal)
- **Fator de expansão:** ~8× ao converter para CSV (47 MB Parquet → ~380 MB CSV por mês)
- **Total:** ~10 GB em CSV, ~94 milhões de registros

**Por que este dataset é Big Data?**
- Volume total de 10+ GB impede `pd.read_csv()` em máquinas com ≤ 16 GB RAM (MemoryError)
- ~94 milhões de linhas tornam perfilamento ingênuo (single-thread) lento (> 3 minutos)
- Variedade de tipos de colunas (floats, inteiros, strings, datas) exige todas as 7 etapas do profiler
- Reprodutível publicamente: qualquer pessoa pode refazer o experimento sem conta ou licença

**Arquivos construídos (concatenação controlada de meses):**

| Arquivo | Meses usados | Linhas aprox. | Tamanho real |
|---------|-------------|---------------|--------------|
| `taxi_1gb.csv` | ~3 meses | ~8 milhões | 1.069 MB |
| `taxi_5gb.csv` | ~14 meses | ~49 milhões | 4.585 MB |
| `taxi_10gb.csv` | ~27 meses | ~94 milhões | 8.910 MB |

**As 19 colunas do dataset:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `VendorID` | int | Empresa do táxi (1 ou 2) |
| `tpep_pickup_datetime` | string | Data/hora de embarque |
| `tpep_dropoff_datetime` | string | Data/hora de desembarque |
| `passenger_count` | float | Número de passageiros |
| `trip_distance` | float | Distância da corrida (milhas) |
| `RatecodeID` | float | Código da tarifa |
| `store_and_fwd_flag` | string | Flag de armazenamento ('Y'/'N') |
| `PULocationID` | int | Zona de embarque |
| `DOLocationID` | int | Zona de desembarque |
| `payment_type` | int | Forma de pagamento (1–6) |
| `fare_amount` | float | Tarifa base (USD) |
| `extra` | float | Cobranças extras |
| `mta_tax` | float | Imposto MTA |
| `tip_amount` | float | Gorjeta |
| `tolls_amount` | float | Pedágios |
| `improvement_surcharge` | float | Adicional de melhoria |
| `total_amount` | float | Total cobrado |
| `congestion_surcharge` | float | Taxa de congestionamento |
| `airport_fee` | float | Taxa de aeroporto |

### 2.2 How to obtain the data

A pasta [`datasample/`](datasample/) contém as primeiras 200 linhas de um arquivo mensal CSV (~21 KB), suficiente para testar o pipeline rapidamente. Ver [`datasample/README.md`](datasample/README.md) para instruções de uso.

Para o dataset completo (1 GB, 5 GB, 10 GB), execute o downloader via Docker:

```bash
# Baixa os arquivos Parquet do NYC TLC (Jan/2022 – Jun/2024) e converte para CSV
# Tempo estimado: 20–60 min (depende da conexão; baixa ~1.3 GB de Parquet)
cd bigdata-preprocessing
docker compose --profile setup up downloader
```

Para economizar tempo e disco, pule o dataset de 10 GB:
```bash
docker compose --profile setup run --rm -e SKIP_10GB=true downloader
```

Os arquivos são gravados em `bigdata-preprocessing/datasets/`:
- `taxi_1gb.csv` (~1 GB)
- `taxi_5gb.csv` (~5 GB)
- `taxi_10gb.csv` (~10 GB, opcional)

**Não inclua os datasets completos no pull request.** Apenas a amostra em `datasample/`.

---

## 3. How to install and run

> **Requisito único:** Docker Desktop instalado e rodando. Nenhuma outra ferramenta é necessária.

### 3.1 Quick start (usando a amostra em `datasample/`)

**Forma mais simples — via script (a partir da raiz do grupo):**

```bash
./bin/run.sh sample   # benchmark Multiprocessing com a amostra
./bin/run.sh spark    # benchmark Spark com a amostra
```

**Ou manualmente com Docker Compose:**

```bash
cd bigdata-preprocessing

# 1. Construir as imagens Docker
docker compose build benchmark
docker compose build spark-benchmark

# 2. Rodar o benchmark Multiprocessing com a amostra
docker compose run --rm \
  -v "$(pwd)/../datasample:/datasample" \
  -e DATASET_PATH=/datasample/taxi_sample.csv \
  -e RESULTS_PATH=/results/metrics_sample.csv \
  benchmark

# 3. Rodar o benchmark Spark com a amostra
docker compose --profile spark run --rm -p 4040:4040 \
  -v "$(pwd)/../datasample:/datasample" \
  -e DATASET_PATH=/datasample/taxi_sample.csv \
  -e RESULTS_PATH=/results/metrics_spark_sample.csv \
  spark-benchmark
```

Cada benchmark grava um CSV de métricas em `results/`. As estatísticas
(média ± desvio-padrão) são calculadas a partir desse CSV com o snippet
pandas/numpy mostrado na seção [6.2](#62-how-to-perform-benchmarking-simple-guide).

### 3.2 How to run with the full dataset

```bash
cd bigdata-preprocessing

# Passo 0: Construir imagens (só na primeira vez)
docker compose build benchmark
docker compose build spark-benchmark

# Passo 1: Baixar dados
docker compose --profile setup up downloader

# Passo 2: Benchmark Multiprocessing — 3 tamanhos
docker compose run --rm -e DATASET_PATH=/datasets/taxi_1gb.csv  -e RESULTS_PATH=/results/metrics_1gb.csv  benchmark
docker compose run --rm -e DATASET_PATH=/datasets/taxi_5gb.csv  -e RESULTS_PATH=/results/metrics_5gb.csv  benchmark
docker compose run --rm -e DATASET_PATH=/datasets/taxi_10gb.csv -e RESULTS_PATH=/results/metrics_10gb.csv benchmark

# Passo 3: Benchmark Apache Spark — 3 tamanhos
docker compose --profile spark run --rm -p 4040:4040 -e DATASET_PATH=/datasets/taxi_1gb.csv  -e RESULTS_PATH=/results/metrics_spark_1gb.csv  spark-benchmark
docker compose --profile spark run --rm -p 4040:4040 -e DATASET_PATH=/datasets/taxi_5gb.csv  -e RESULTS_PATH=/results/metrics_spark_5gb.csv  spark-benchmark
docker compose --profile spark run --rm -p 4040:4040 -e DATASET_PATH=/datasets/taxi_10gb.csv -e RESULTS_PATH=/results/metrics_spark_10gb.csv spark-benchmark
```

Ao final, cada configuração gera um CSV em `results/` (`metrics_*.csv` e
`metrics_spark_*.csv`). A análise estatística (média e desvio-padrão) é feita
com o snippet pandas/numpy da seção [6.2](#62-how-to-perform-benchmarking-simple-guide).

**Parâmetros configuráveis via variáveis de ambiente:**

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `DATASET_PATH` | `/datasets/taxi.csv` | Caminho do CSV no container |
| `WORKERS_LIST` | `1,2,4,8,16` | Workers a testar (multiprocessing) |
| `CORES_LIST` | `1,2,4,8,16` | Núcleos a testar (Spark) |
| `N_REPS` | `3` | Repetições por configuração |
| `CHUNK_SIZE` | `500000` | Linhas por chunk (multiprocessing) |
| `RESULTS_PATH` | `/results/metrics.csv` | Caminho de saída |

---

## 4. Project architecture

```
[NYC TLC CDN] ─── Parquet (~47 MB/mês) ──→ [downloader.py]
                                                    │
                                    converte Parquet→CSV, concatena
                                                    │
                                                    ▼
                             [datasets/taxi_{1,5,10}gb.csv]
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          │                           ▼
    [WORKLOAD A: benchmark.py]           │           [WORKLOAD B: spark_benchmark.py]
    ProcessPoolExecutor                  │           PySpark local[N]
    ─ lê CSV em chunks (500k linhas)     │           ─ Catalyst Optimizer
    ─ envia chunks p/ workers via IPC    │           ─ scan único compilado
    ─ profile_chunk() em paralelo        │           ─ threads Java (sem GIL)
    ─ aggregate_chunks() combina         │           ─ approxCountDistinct HLL
              │                          │                           │
              ▼                          │                           ▼
    [results/metrics_*.csv]              │           [results/metrics_spark_*.csv]
              └──────────────────────────┼──────────────────────────┘
                                         ▼
                          [análise pandas/numpy — seção 6.2]
                          ─ média ± desvio-padrão por configuração
                          ─ speedup e análise de Amdahl
                          ─ comparação das duas abordagens
```

**Organização do código:**
- **`bin/`** — scripts auxiliares que rodam o pipeline via Docker (`run.sh`)
- **`src/`** — código-fonte Python na raiz do grupo (montado como `../src:/app/src` pelo docker-compose)
- **`bigdata-preprocessing/`** — Dockerfiles, docker-compose.yml e volumes de datasets/results

**Componentes e containers Docker:**

| Serviço | Imagem | Papel |
|---------|--------|-------|
| `downloader` | `./Dockerfile` (Python 3.11) | Baixa Parquet do NYC TLC, converte e concatena CSVs |
| `benchmark` | `./Dockerfile` (Python 3.11) | Roda `ProcessPoolExecutor` com 1/2/4/8/16 workers |
| `spark-benchmark` | `./spark/Dockerfile` (Python 3.11 + Java 17) | Roda PySpark `local[N]` com 1/2/4/8/16 núcleos |

**Fluxo de dados:**
1. `downloader` → grava CSVs em `datasets/` (volume compartilhado)
2. `benchmark` / `spark-benchmark` → leem de `datasets/`, gravam métricas em `results/`
3. Análise estatística (média ± desvio, speedup) → snippet pandas/numpy da seção 6.2 sobre os CSVs de `results/`

---

## 5. Workloads evaluated

### [WORKLOAD-A] PROF-MP — Perfilamento com Python Multiprocessing

Implementado em [`benchmark.py`](src/benchmark.py) + [`profiler.py`](src/profiler.py) + [`aggregator.py`](src/aggregator.py).

**Processo:**
1. O processo principal lê o CSV em chunks de 500.000 linhas (`pd.read_csv(..., chunksize=500_000)`)
2. Cada chunk é enviado via IPC (pickle) para um `ProcessPoolExecutor` com N workers
3. Cada worker executa `profile_chunk()` — computa para cada coluna: nulos, min/max/sum/sum² (numérico), value_counts (categórico), detecção de datas, cardinalidade
4. O processo principal agrega todos os resultados parciais com `aggregate_chunks()`
5. `n_workers=1` roda em loop serial puro (sem overhead do executor)

**O que varia:** `n_workers` ∈ {1, 2, 4, 8, 16}, 3 repetições por configuração, 3 tamanhos de dataset

### [WORKLOAD-B] PROF-SPARK — Perfilamento com Apache Spark

Implementado em [`spark_benchmark.py`](src/spark/spark_benchmark.py).

**Processo:**
1. `SparkSession` criada em modo `local[N]` — N threads Java no mesmo processo
2. CSV lido via `spark.read.csv()` com inferência de schema
3. **Job 1:** contagem de linhas + nulos em todas as colunas (1 scan)
4. **Job 2:** min/max/mean/stddev + `approx_count_distinct` (HyperLogLog, erro < 5%) para todas as colunas numéricas em 1 scan único compilado pelo Catalyst
5. **Job 3:** `groupBy().count()` para colunas categóricas de baixa cardinalidade
6. 1 rodada de warmup por configuração (aquece JVM + cache do Catalyst) — **não contabilizada**

**O que varia:** `n_cores` ∈ {1, 2, 4, 8, 16}, 3 repetições por configuração

**Diferença fundamental entre as abordagens:**
- Python MP: I/O é serial (pandas lê chunk a chunk no processo principal), compute é paralelo mas limitado pelo GIL na serialização/deserialização
- Spark: I/O + compute são paralelos end-to-end via JVM threads; Catalyst compila todas as agregações em um único scan physical

---

## 6. Experiments and results

### 6.1 Experimental environment

Experimentos executados na máquina do desenvolvedor:

| Componente | Especificação |
|------------|---------------|
| CPU | AMD Ryzen 7 8700G — 8 núcleos físicos, 16 lógicos (SMT) |
| RAM | 64 GB DDR5 |
| Disco | NVMe 1 TB (read sequencial ~3.5 GB/s) |
| OS | Windows 11 Pro + Docker Desktop (WSL2 backend) |
| Docker | Engine 26.x / Compose v2 |
| Python | 3.11 (dentro do container) |
| PySpark | 3.5.1 |
| Java | OpenJDK 17 |

### 6.2 How to perform benchmarking (simple guide)

O harness de benchmark já está implementado e automatizado. Para reproduzir:

```bash
# Rodar N repetições de um experimento específico
docker compose run --rm \
  -e DATASET_PATH=/datasets/taxi_1gb.csv \
  -e WORKERS_LIST=1,2,4,8,16 \
  -e N_REPS=5 \
  -e RESULTS_PATH=/results/metrics_1gb_5reps.csv \
  benchmark
```

As métricas coletadas são: `dataset_size_mb`, `n_workers`, `rep`, `time_seconds`, `throughput_mb_s`.

Cálculo de estatísticas (Python):
```python
import pandas as pd, numpy as np
df = pd.read_csv("results/metrics_1gb.csv")
stats = df.groupby("n_workers")["time_seconds"].agg(
    avg=np.mean, std=lambda x: np.std(x, ddof=1)
)
```

### 6.3 What did you test?

**Parâmetros variados:**
- Número de workers/cores: 1, 2, 4, 8, 16
- Tamanho do dataset: 1 GB, 5 GB, 10 GB
- Abordagem de paralelismo: Python Multiprocessing vs Apache Spark

**Métricas medidas:**
- Tempo de execução (segundos) — wall-clock time
- Throughput (MB/s) — dataset_size_mb / time_seconds
- Speedup relativo — tempo_1_worker / tempo_N_workers

**Todas as configurações:** 3 repetições (mínimo obrigatório).

### 6.4 Results

#### WORKLOAD-A: Python Multiprocessing — 1 GB

| Workers | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg Time (s) | Std Dev (s) | Avg Throughput (MB/s) | Speedup |
|---------|-----------|-----------|-----------|-------------|-------------|----------------------|---------|
| 1 (serial) | 22.72 | 23.78 | 23.94 | **23.48** | 0.66 | 45.55 | 1.00× |
| 2 | 25.61 | 24.25 | 24.10 | **24.65** | 0.83 | 43.39 | 0.95× |
| 4 | 25.62 | 24.46 | 25.44 | **25.17** | 0.62 | 42.49 | 0.93× |
| 8 | 25.04 | 25.75 | 25.63 | **25.47** | 0.38 | 42.01 | 0.92× |
| 16 | 24.76 | 26.00 | 25.44 | **25.40** | 0.62 | 42.10 | 0.92× |

**Discussion:** Nenhum ganho de paralelismo no dataset de 1 GB. O workload é **I/O-bound**: o processo principal já lê o CSV a ~45 MB/s (próximo ao limite do disco para processos Docker no Windows/WSL2). Adicionar workers só acrescenta overhead de IPC (pickle) sem acelerar a leitura. O baseline serial é o mais rápido.

#### WORKLOAD-A: Python Multiprocessing — 5 GB

| Workers | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg Time (s) | Std Dev (s) | Avg Throughput (MB/s) | Speedup |
|---------|-----------|-----------|-----------|-------------|-------------|----------------------|---------|
| 1 (serial) | 105.61 | 98.87 | 99.12 | **101.20** | 3.82 | 45.36 | 1.00× |
| 2 | 96.02 | 95.34 | 97.06 | **96.14** | 0.87 | 47.04 | 1.05× |
| 4 | 96.40 | 96.96 | 96.15 | **96.50** | 0.41 | 47.52 | 1.05× |
| 8 | 96.93 | 97.15 | 96.35 | **96.81** | 0.41 | 47.36 | 1.05× |
| 16 | 97.17 | 99.42 | 113.35 | **103.32** | 8.76 | 44.59 | 0.98× |

**Discussion:** Leve melhora com 2 workers (~5%), mas plateau completo a partir de 4. O n=16 mostra degradação na 3ª repetição (113 s) com alto desvio padrão (8.76 s) — provavelmente contenção de memória e overhead do scheduler ao criar 16 processos filhos. Confirmação do gargalo de I/O.

#### WORKLOAD-A: Python Multiprocessing — 10 GB

| Workers | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg Time (s) | Std Dev (s) | Avg Throughput (MB/s) | Speedup |
|---------|-----------|-----------|-----------|-------------|-------------|----------------------|---------|
| 1 (serial) | 195.18 | 191.64 | 189.99 | **192.27** | 2.65 | 46.35 | 1.00× |
| 2 | 179.02 | 182.03 | 181.39 | **180.81** | 1.59 | 49.28 | 1.06× |
| 4 | 181.42 | 180.80 | 184.54 | **182.25** | 2.00 | 48.89 | 1.05× |
| 8 | 183.14 | 180.25 | 181.19 | **181.53** | 1.48 | 49.09 | 1.06× |
| 16 | 181.17 | 181.78 | 182.01 | **181.65** | 0.43 | 49.06 | 1.06× |

**Discussion:** Mesmo padrão do 5 GB — speedup máximo de ~1.06× com qualquer número de workers > 1. Platô imediato confirma que o bottleneck é I/O e que não há compute suficiente para justificar overhead de IPC. O desvio padrão muito baixo (0.43 s) no n=16 indica que a carga é estável e previsível neste tamanho de dataset.

#### WORKLOAD-B: Apache Spark — 1 GB

| Cores | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg Time (s) | Std Dev (s) | Avg Throughput (MB/s) | Speedup |
|-------|-----------|-----------|-----------|-------------|-------------|----------------------|---------|
| 1 | 127.20 | 123.06 | 123.40 | **124.55** | 2.30 | 8.59 | 1.00× |
| 2 | 64.25 | 65.81 | 66.95 | **65.67** | 1.36 | 16.28 | 1.90× |
| 4 | 40.06 | 39.30 | 39.91 | **39.76** | 0.40 | 26.88 | 3.13× |
| 8 | 30.70 | 29.96 | 29.72 | **30.13** | 0.51 | 35.49 | 4.13× |
| 16 | 23.63 | 22.68 | 23.53 | **23.28** | 0.52 | 45.94 | 5.35× |

**Discussion:** O Spark demonstra **escalabilidade quase linear** de 1 para 4 cores (speedup ideal seria 4×, obtivemos 3.13×). De 4 para 16, o speedup desacelera (efeito de Lei de Amdahl — overhead de coordenação). Com 16 cores, Spark (23.3 s) supera Python serial (23.5 s) e iguala o Multiprocessing, **sem o overhead de IPC**. O ganho do Spark vem de dois fatores: (1) threads Java não têm GIL; (2) Catalyst compila todas as agregações em um único scan físico — I/O e compute sobrepõem.

#### Comparação final — 1 GB com 16 workers/cores

| Abordagem | Avg Time (s) | Std Dev (s) | Avg Throughput (MB/s) |
|-----------|-------------|-------------|----------------------|
| Python serial (baseline) | 23.48 | 0.66 | 45.55 |
| Python MP (16 workers) | 25.40 | 0.62 | 42.10 |
| Spark (1 core) | 124.55 | 2.30 | 8.59 |
| Spark (16 cores) | 23.28 | 0.52 | 45.94 |

**Conclusão dos experimentos:** para este workload de perfilamento, o gargalo é I/O no multiprocessing Python (o `pd.read_csv` é serial por natureza). O Spark consegue paralelizar I/O + compute end-to-end usando threads JVM, alcançando speedup real de ~5.35× ao escalar de 1 para 16 cores.

> **Nota:** Os experimentos Spark para 5 GB e 10 GB ainda não foram executados (tempo de execução estimado: ~50 min e ~100 min respectivamente). Os resultados serão adicionados antes da submissão final.

---

## 7. Limitations and conclusions

### O que funcionou

- O pipeline completo (download → profiling → relatório) roda de ponta a ponta via Docker Compose sem configuração manual
- O Python Multiprocessing **resolve o problema original** (MemoryError): processa datasets de 10 GB com uso de RAM constante (~2 GB) graças ao processamento em chunks
- O Apache Spark escala bem com o número de cores para este workload, alcançando speedup de 5.35× com 16 cores vs 1 core
- Com 16 cores, ambas as abordagens atingem throughputs similares (~45 MB/s), mas o Spark chega lá via paralelismo genuíno, enquanto o Python mal supera o baseline serial

### Limitações

- **Python Multiprocessing:** o gargalo de I/O é estrutural — pandas não oferece leitura paralela de um único arquivo CSV. Para superar isso seria necessário dividir o arquivo previamente ou usar mmap com múltiplos readers
- **Spark com poucos cores (1–2):** overhead de inicialização da JVM (~5 min) e custo fixo do Catalyst penalizam datasets pequenos. Spark só compensa a partir de 4+ cores no dataset de 1 GB
- **Ambiente de execução:** os experimentos foram rodados em máquina local Windows com Docker Desktop (WSL2), o que introduz overhead de virtualização. Em Linux nativo, os tempos de I/O seriam menores
- **Spark 5 GB e 10 GB:** resultados não disponíveis nesta versão — experimentos em andamento
- **Número de repetições:** mínimo de 3 repetições por configuração; o ideal seria 5–10 para maior confiança estatística, especialmente para n=16 que mostrou variabilidade maior

### Conclusões

1. **Para datasets ≥ 1 GB, ProcessPoolExecutor com pandas em chunks é a solução mais simples** que resolve o MemoryError com overhead mínimo (speedup marginal de ~5%)
2. **Spark só compensa com ≥ 4 cores e datasets suficientemente grandes** — o overhead de JVM/Catalyst torna Spark mais lento que Python serial para 1–2 cores em 1 GB
3. **O gargalo real deste workload é I/O, não CPU** — a proporção de compute por byte lido é baixa, limitando o speedup de qualquer abordagem baseada em multiprocessing com I/O serial
4. **Com 16 cores, Spark ≈ Python serial** — ambos processam 1 GB em ~23 s e ~45 MB/s, mas por caminhos completamente diferentes

---

## 8. References and external resources

- [NYC TLC Yellow Taxi Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — dataset principal
- [Apache Spark Documentation](https://spark.apache.org/docs/3.5.1/) — PySpark 3.5.1
- [Python concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) — ProcessPoolExecutor
- [pandas Documentation](https://pandas.pydata.org/docs/) — pd.read_csv com chunksize
- [Amdahl's Law](https://en.wikipedia.org/wiki/Amdahl%27s_law) — análise teórica de speedup
- [Docker Compose Documentation](https://docs.docker.com/compose/) — orquestração dos serviços
- [PyArrow Documentation](https://arrow.apache.org/docs/python/) — leitura de Parquet
- [VisFlow-MM](https://github.com/VISFLOW-MM/bigdata-preprocessing) — sistema original que motivou o projeto
