# Perfilamento Paralelo de Dados — NYC Yellow Taxi

Trabalho da disciplina de **Big Data** · Universidade Federal de Lavras (UFLA)

Implementa e compara duas estratégias de perfilamento paralelo de grandes arquivos CSV:
- **Multiprocessing Python** (`ProcessPoolExecutor`) — leitura em chunks + workers paralelos
- **Apache Spark** (`local[N]`) — Catalyst Optimizer + threads Java sem GIL

Dataset utilizado: [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e **rodando**
- ~15 GB de espaço em disco (para os três datasets: 1 GB + 5 GB + 10 GB)
- Conexão com a internet (para baixar os arquivos parquet do NYC TLC)

Verificar se o Docker está rodando:
```powershell
docker info
```

---

## Estrutura do projeto

```
bigdata-preprocessing/
├── docker-compose.yml          ← orquestra todos os serviços
├── Dockerfile                  ← imagem do benchmark multiprocessing
├── requirements.txt            ← dependências Python (pandas, numpy, etc.)
│
├── src/
│   ├── downloader.py           ← baixa parquet do NYC TLC e converte para CSV
│   ├── profiler.py             ← perfilamento por chunk (lógica do DataSetSummary)
│   ├── aggregator.py           ← merge dos resultados parciais dos chunks
│   └── benchmark.py            ← harness de experimentos (mede tempo e vazão)
│
├── spark/
│   ├── Dockerfile              ← imagem com Python + Java 17 (para PySpark)
│   ├── requirements.txt        ← pyspark, pandas
│   └── src/
│       └── spark_benchmark.py  ← mesmo perfilamento implementado em PySpark
│
├── datasets/                   ← CSVs ficam aqui (gerados pelo downloader)
│   ├── taxi_1gb.csv
│   ├── taxi_5gb.csv
│   ├── taxi_10gb.csv
│   ├── parquet/                ← arquivos parquet originais (cache local)
│   └── monthly_csv/            ← CSVs mensais individuais (intermediários)
│
└── results/                    ← saídas dos experimentos
    ├── metrics_1gb.csv         ← tempos do multiprocessing (1 GB)
    ├── metrics_5gb.csv         ← tempos do multiprocessing (5 GB)
    ├── metrics_10gb.csv        ← tempos do multiprocessing (10 GB)
    ├── metrics_spark_1gb.csv   ← tempos do Spark (1 GB)
    ├── metrics_spark_5gb.csv   ← tempos do Spark (5 GB)
    └── metrics_spark_10gb.csv  ← tempos do Spark (10 GB)
```

> A análise estatística (média ± desvio-padrão, speedup, comparação
> Spark × multiprocessing) é feita sobre esses CSVs com o snippet
> pandas/numpy descrito no README principal (seção 6.2).

---

## Passo 0 — Construir as imagens Docker

Na primeira vez (ou após alterar `Dockerfile` ou `requirements.txt`):

```powershell
# Imagem do benchmark multiprocessing
docker compose build benchmark

# Imagem do benchmark Spark
docker compose build spark-benchmark
```

---

## Passo 1 — Baixar e preparar os dados

Faz o download dos arquivos parquet mensais do NYC TLC (2022–2024),
converte cada um para CSV e monta os três arquivos de tamanho alvo.

> **Tempo estimado:** 20–60 min dependendo da conexão (baixa ~1.3 GB de parquet)

```powershell
docker compose --profile setup up downloader
```

Arquivos gerados em `datasets/`:

| Arquivo | Tamanho | Linhas aprox. | Meses usados |
|---------|---------|---------------|--------------|
| `taxi_1gb.csv` | ~1 GB | ~8 milhões | 3 meses |
| `taxi_5gb.csv` | ~5 GB | ~49 milhões | 14 meses |
| `taxi_10gb.csv` | ~10 GB | ~94 milhões | 27 meses |

Se quiser pular o dataset de 10 GB (economiza tempo e disco):
```powershell
docker compose --profile setup run --rm -e SKIP_10GB=true downloader
```

---

## Passo 2 — Benchmark Multiprocessing

Executa o perfilamento com `ProcessPoolExecutor` testando 1, 2, 4, 8 e 16 workers,
3 repetições cada. O `n_workers=1` roda em loop serial puro (baseline).

### 2.1 — Dataset de 1 GB

```powershell
docker compose run --rm `
  -e DATASET_PATH=/datasets/taxi_1gb.csv `
  -e RESULTS_PATH=/results/metrics_1gb.csv `
  benchmark
```

> **Tempo estimado:** ~7 min (5 configurações × 3 repetições × ~23 s cada)

### 2.2 — Dataset de 5 GB

```powershell
docker compose run --rm `
  -e DATASET_PATH=/datasets/taxi_5gb.csv `
  -e RESULTS_PATH=/results/metrics_5gb.csv `
  benchmark
```

> **Tempo estimado:** ~30 min (~100 s por execução)

### 2.3 — Dataset de 10 GB

```powershell
docker compose run --rm `
  -e DATASET_PATH=/datasets/taxi_10gb.csv `
  -e RESULTS_PATH=/results/metrics_10gb.csv `
  benchmark
```

> **Tempo estimado:** ~50 min (~190 s por execução)

### Parâmetros configuráveis do benchmark

Todos podem ser passados via `-e` no `docker compose run`:

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `DATASET_PATH` | `/datasets/taxi.csv` | Caminho do CSV dentro do container |
| `WORKERS_LIST` | `1,2,4,8,16` | Lista de workers a testar |
| `N_REPS` | `3` | Repetições por configuração |
| `CHUNK_SIZE` | `500000` | Linhas por chunk |
| `RESULTS_PATH` | `/results/metrics.csv` | Onde salvar os resultados |

Exemplo — rodar só com 4 e 8 workers, 5 repetições:
```powershell
docker compose run --rm `
  -e DATASET_PATH=/datasets/taxi_1gb.csv `
  -e WORKERS_LIST=4,8 `
  -e N_REPS=5 `
  -e RESULTS_PATH=/results/metrics_1gb_custom.csv `
  benchmark
```

---

## Passo 3 — Benchmark Apache Spark

Executa o mesmo perfilamento com PySpark em modo `local[N]`,
testando 1, 2, 4, 8 e 16 núcleos. Inclui 1 rodada de warmup (aquecimento da JVM)
antes de cada série de repetições — o warmup **não** é contabilizado nos resultados.

A interface web do Spark fica disponível em **http://localhost:4040** enquanto o job roda.

### 3.1 — Dataset de 1 GB

```powershell
docker compose --profile spark run --rm -p 4040:4040 `
  -e DATASET_PATH=/datasets/taxi_1gb.csv `
  -e RESULTS_PATH=/results/metrics_spark_1gb.csv `
  spark-benchmark
```

> **Tempo estimado:** ~15 min (JVM warmup + 5 configurações × 3 reps + warmup por config)

### 3.2 — Dataset de 5 GB

```powershell
docker compose --profile spark run --rm -p 4040:4040 `
  -e DATASET_PATH=/datasets/taxi_5gb.csv `
  -e RESULTS_PATH=/results/metrics_spark_5gb.csv `
  spark-benchmark
```

> **Tempo estimado:** ~50 min

### 3.3 — Dataset de 10 GB

```powershell
docker compose --profile spark run --rm -p 4040:4040 `
  -e DATASET_PATH=/datasets/taxi_10gb.csv `
  -e RESULTS_PATH=/results/metrics_spark_10gb.csv `
  spark-benchmark
```

> **Tempo estimado:** ~100 min

### Parâmetros configuráveis do Spark

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `DATASET_PATH` | `/datasets/taxi.csv` | Caminho do CSV |
| `CORES_LIST` | `1,2,4,8,16` | Núcleos a testar (`local[N]`) |
| `N_REPS` | `3` | Repetições (após 1 warmup) |
| `RESULTS_PATH` | `/results/metrics_spark.csv` | Onde salvar |
| `DRIVER_MEM` | `4g` | Memória do driver Spark |

---

## Passo 4 — Análise estatística dos resultados

Cada execução dos passos 2 e 3 grava um CSV de métricas em `results/`
(`metrics_*.csv` e `metrics_spark_*.csv`). A partir desses CSVs calcule
média e desvio-padrão por configuração:

```python
import pandas as pd, numpy as np
df = pd.read_csv("results/metrics_1gb.csv")
stats = df.groupby("n_workers")["time_seconds"].agg(
    avg=np.mean, std=lambda x: np.std(x, ddof=1)
)
print(stats)
```

As tabelas consolidadas (média ± desvio, speedup e comparação
Spark × multiprocessing) estão na seção 6 do README principal.

---

## Sequência completa (todos os passos em ordem)

```powershell
# 0. Construir imagens
docker compose build benchmark
docker compose build spark-benchmark

# 1. Baixar dados
docker compose --profile setup up downloader

# 2. Multiprocessing — 1 GB
docker compose run --rm -e DATASET_PATH=/datasets/taxi_1gb.csv  -e RESULTS_PATH=/results/metrics_1gb.csv  benchmark

# 3. Multiprocessing — 5 GB
docker compose run --rm -e DATASET_PATH=/datasets/taxi_5gb.csv  -e RESULTS_PATH=/results/metrics_5gb.csv  benchmark

# 4. Multiprocessing — 10 GB
docker compose run --rm -e DATASET_PATH=/datasets/taxi_10gb.csv -e RESULTS_PATH=/results/metrics_10gb.csv benchmark

# 5. Spark — 1 GB
docker compose --profile spark run --rm -p 4040:4040 -e DATASET_PATH=/datasets/taxi_1gb.csv  -e RESULTS_PATH=/results/metrics_spark_1gb.csv  spark-benchmark

# 6. Spark — 5 GB
docker compose --profile spark run --rm -p 4040:4040 -e DATASET_PATH=/datasets/taxi_5gb.csv  -e RESULTS_PATH=/results/metrics_spark_5gb.csv  spark-benchmark

# 7. Spark — 10 GB
docker compose --profile spark run --rm -p 4040:4040 -e DATASET_PATH=/datasets/taxi_10gb.csv -e RESULTS_PATH=/results/metrics_spark_10gb.csv spark-benchmark

# 8. Análise estatística — calcule média ± desvio a partir dos CSVs em results/
#    (veja o snippet pandas/numpy no Passo 4)
```

---

## Interface Web do Spark (Spark UI)

Disponível **somente enquanto um job Spark está rodando**.

Acesse: **http://localhost:4040**

| Aba | O que ver |
|-----|-----------|
| **Jobs** | Lista de jobs executados (count, agg, groupBy) |
| **Stages** | Fases de cada job: leitura de disco, compute, shuffle |
| **Executors** | Threads ativas, memória usada, tasks concluídas |
| **SQL** | Plano de execução do Catalyst Optimizer (grafo de operações) |

> A aba **SQL** é a mais útil — mostra que todas as agregações numéricas
> (min, max, mean, stddev) são compiladas em **um único scan** do arquivo.

---

## Verificar resultados gerados

```powershell
# Listar arquivos de métricas
ls .\results\*.csv

# Ver primeiras linhas de um CSV
Get-Content .\results\metrics_1gb.csv | Select-Object -First 5
```

Formato do CSV de métricas:

```
dataset_size_mb,n_workers,rep,time_seconds,throughput_mb_s
1069.0,1,1,22.71,47.05
1069.0,1,2,23.78,44.94
...
```

---

## Resolução de problemas

| Erro | Causa | Solução |
|------|-------|---------|
| `unable to get image` / `check if daemon is running` | Docker Desktop fechado | Abrir o Docker Desktop e aguardar ficar verde |
| `no such service: benchmark` | Rodando na pasta errada | `cd bigdata-preprocessing` antes do comando |
| `Unable to locate package openjdk-17` | Imagem Spark desatualizada | A imagem usa `python:3.11-slim-bookworm` — rebuildar: `docker compose build spark-benchmark` |
| `DtypeWarning: Columns have mixed types` | Aviso do pandas (não é erro) | Ignorar — já tratado com `low_memory=False` |
| `FileNotFoundError: taxi_1gb.csv` | Dataset não baixado | Rodar o **Passo 1** (downloader) primeiro |
| `JAVA_HOME` não encontrado | Variável de ambiente faltando | Rebuildar a imagem Spark: `docker compose build spark-benchmark` |

---

## Tecnologias utilizadas

| Tecnologia | Versão | Uso |
|------------|--------|-----|
| Python | 3.11 | Linguagem principal |
| pandas | 2.2.2 | Leitura de CSV e profiling serial |
| numpy | 1.26.4 | Operações numéricas |
| pyarrow | 16.0.0 | Leitura de arquivos Parquet |
| PySpark | 3.5.1 | Benchmark Apache Spark |
| Java (OpenJDK) | 17 | JVM para o Spark |
| matplotlib | 3.9.0 | Geração opcional de gráficos a partir das métricas |
| Docker / Compose | — | Containerização e reprodutibilidade |
