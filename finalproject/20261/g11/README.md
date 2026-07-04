# Final project report: *Pipeline de Stream Processing sobre NYC TLC Trip Record Data*

> Projeto final — Big Data, Ciência da Computação, UFLA (Grupo **g11**).
>
> Este `README.md` é auto-suficiente e cobre as 8 seções obrigatórias. O
> **relatório técnico completo** (fundamentação teórica, decisões de projeto e
> análise detalhada) está  em
> [`presentation/RELATORIO.md`](presentation/RELATORIO.md).

## 1. Context and motivation

O objetivo é construir um **pipeline de processamento de fluxo (stream) distribuído**
sobre dados reais de corridas de táxi de Nova York e **avaliar empiricamente seu
desempenho** — primeiro num host único, depois num cluster real de 4 VMs na AWS.

Em vez de tratar os dados como um arquivo estático (batch), tratamo-los como um
**fluxo contínuo de eventos** (um _firehose_): um produtor injeta corridas a alta
vazão num broker Kafka, e o Apache Spark Structured Streaming as consome, filtra e
agrega em tempo (quase) real. O problema de Big Data central é o **shuffle**: as
agregações _stateful_ (janela por tempo + join com a tabela de zonas) reagrupam
registros por chave e, num sistema distribuído, obrigam a redistribuir dados pela
rede — o gargalo clássico do Big Data, cujo custo cresce com o volume **e** com o
número de nós. A pergunta experimental é: **adicionar máquinas realmente acelera o
pipeline?**

## 2. Data

### 2.1 Detailed description

Usamos o **NYC TLC Trip Record Data**, dados públicos e reais da _New York City Taxi
& Limousine Commission_:
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.

A fonte reúne **quatro serviços** com **schemas heterogêneos** (colunas de
event-time diferentes), distribuídos em **formato Parquet** (colunar, comprimido):

| Serviço | Modalidade | Coluna de event-time | Campo de tarifa |
|---|---|---|---|
| `yellow` | táxi amarelo (medallion) | `tpep_pickup_datetime` | `fare_amount` |
| `green`  | táxi verde (boro taxi)   | `lpep_pickup_datetime` | `fare_amount` |
| `fhv`    | for-hire vehicle         | `pickup_datetime`      | _(ausente)_ |
| `fhvhv`  | high-volume FHV (Uber/Lyft) | `pickup_datetime`   | `base_passenger_fare` |

A TLC publica **anos de dados** (dezenas de GB no total). Para este trabalho baixamos
originalmente um recorte de **4 meses** (os quatro serviços), somando **≈ 2,3 GB /
≈ 102 milhões de linhas** — o que já corresponde ao `data/` completo usado localmente.
Desses 4 meses, os experimentos das seções 6 usaram apenas **2 meses**, um
**subconjunto de ≈ 48,8 milhões de linhas / 1,07 GB** (os quatro serviços), por
reprodutibilidade dentro dos recursos disponíveis. O produtor
([`src/producer/producer.py`](src/producer/producer.py)) lê cada Parquet, **normaliza
toda linha para um evento canônico único** (unificando os 4 schemas num só, com um
único watermark) e publica no tópico Kafka.

**Por que é Big Data:** *(i)* **Volume** — o subconjunto já ocupa 1,07 GB comprimido
e não cabe confortavelmente na RAM, forçando processamento em partições; *(ii)*
**Velocidade** — fluxo contínuo de alta vazão, não um arquivo estático; *(iii)*
**Custo de shuffle** — as agregações exigem redistribuição de dados pela rede.

### 2.2 How to obtain the data

- **Amostra (incluída):** [`datasample/`](datasample/) contém 4 Parquets reais
  (yellow/green/fhv/fhvhv, jan/2025), **~566 KB no total** (< 1 MB) — 21.000 corridas.
  É o que o _quick start_ usa; nenhum download é necessário para testar o projeto.

- **Dataset completo (não incluído):** baixe os Parquets mensais direto da TLC. Ex.:

  ```bash
  mkdir -p data
  # ajuste ano/mes e serviço conforme a necessidade:
  wget -P data/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
  wget -P data/ https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet
  wget -P data/ https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2025-01.parquet
  wget -P data/ https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet
  ```

  Os links oficiais (por mês/serviço) estão na página da TLC citada acima.

- **Como a amostra foi gerada:** ela é derivada dos Parquets completos truncando as
  primeiras N linhas de cada serviço (6.000 para yellow/green/fhv, 3.000 para o
  volumoso fhvhv), mantendo todas as colunas originais. Para regerá-la a partir de um
  `data/` completo, com Docker (sem instalar Python no host):

  ```bash
  docker run --rm -v "$PWD/data:/in:ro" -v "$PWD/datasample:/out" python:3.11-slim \
    sh -c "pip install -q pyarrow pandas && python - <<'PY'
  import pyarrow.parquet as pq, pyarrow as pa
  for svc,n in {'yellow':6000,'green':6000,'fhv':6000,'fhvhv':3000}.items():
      f=f'{svc}_tripdata_2025-01.parquet'
      b=next(pq.ParquetFile(f'/in/{f}').iter_batches(batch_size=n))
      pq.write_table(pa.Table.from_batches([b])[:n], f'/out/{f}', compression='snappy')
      print(svc, n)
  PY"
  ```

## 3. How to install and run

> **Requisito estrito:** o único pré-requisito é **Docker** (com Compose v2). O
> produtor e o job Spark rodam **inteiramente em contêineres** — nenhum Python, venv
> ou dependência precisa ser instalado no host.

### 3.1 Quick start (using sample data in `datasample/`)

```bash
cd g11
bin/run.sh
```

O script executa o pipeline completo, do zero, só com Docker:

1. sobe a infraestrutura (Kafka em modo KRaft + Spark master + 1 worker);
2. espera o Kafka ficar _healthy_;
3. **ingere a amostra** rodando o contêiner `producer` sobre `datasample/`;
4. **submete o job** de Structured Streaming (baixa o conector Kafka na 1ª vez);
5. escreve os resultados em `output/daily_peaks/` e `output/avg_duration_by_zone/`.

Para derrubar tudo ao final: `docker compose -f misc/docker-compose.yml down -v`.

### 3.2 How to run with the full dataset

Baixe o dataset completo em `data/` (§2.2) e aponte o mount para ele via
`DATA_DIR_HOST` (relativo a `misc/`):

```bash
cd g11
DATA_DIR_HOST=../data bin/run.sh --interleave        # ou --max-records N p/ limitar
```

Para escalar workers manualmente (Cenário B do experimento):

```bash
docker compose -f misc/docker-compose.yml up -d --scale spark-worker=3
```

## 4. Project architecture

Fluxo linear: **producer → Kafka → Spark Structured Streaming → Parquet**, tudo
orquestrado em Docker. As duas fases experimentais usam **o mesmo pipeline lógico**,
mudando apenas o substrato (Compose no host único; Swarm no cluster AWS).

```text
   datasample/ | data/  (Parquet · 4 serviços · schemas heterogêneos)
      │  lê em batches (pyarrow) · normaliza p/ EVENTO CANÔNICO (JSON)
      ▼
 ┌──────────────┐   key = service_type → partição por hash
 │  producer.py │   alta vazão: lz4 + batching + acks=1
 └──────┬───────┘
        │  publish → taxi_trips_stream
        ▼
 ┌─────────────────────────────────┐   pub/sub · commit log persistente
 │  Apache Kafka (KRaft, 1 broker)  │   tópico · 12 partições
 └──────┬──────────────────────────┘
        │  readStream (kafka:9092 · startingOffsets=earliest)
        ▼
 ┌──────────────────────────────────────────────────────┐
 │  Spark Structured Streaming  (src/spark/stream_job.py) │
 │    from_json → schema canônico · withWatermark         │
 │    filtro de anomalias · agregações STATEFUL (SHUFFLE):│
 │      (a) picos diários   window(1 dia).count()         │
 │      (b) tempo médio/zona groupBy(1 dia, zona).avg()   │
 │                            + JOIN tabela de zonas       │
 │  Spark Master  +  N Spark Workers (escaláveis)         │
 └──────┬─────────────────────────────────────────────────┘
        │  writeStream (Parquet, append + checkpoint)
        ▼
   output/{daily_peaks, avg_duration_by_zone}
```

**Componentes e contêineres** (ver [`misc/docker-compose.yml`](misc/docker-compose.yml)):

| Componente | Contêiner / arquivo | Papel |
|---|---|---|
| Ingestão | `producer` (`src/producer/`, Dockerfile) | lê Parquet, normaliza, publica no Kafka |
| Mensageria | `kafka` (KRaft, 1 broker, 12 partições) | _firehose_ pub/sub, buffer durável |
| Processamento | `spark-master` + `spark-worker` (escalável) | consome, filtra, agrega (shuffle), escreve |
| Join | `misc/zones/taxi_zone_lookup.csv` | tabela de zonas de NYC (borough/zone) |
| Saída | `output/` (bind-mount rw) | resultados Parquet + checkpoints do streaming |

## 5. Workloads evaluated

- **[WORKLOAD-PREP] Normalização para o evento canônico.** Pré-processamento no
  producer: mapeia os 4 schemas heterogêneos para um schema único
  (`service_type, pickup_datetime, dropoff_datetime, pu/do_location_id, trip_distance,
  fare_amount, trip_duration_s`), converte tipos e calcula a duração — permitindo um
  stream uniforme com um único watermark.
- **[WORKLOAD-1] Picos diários (`daily_peaks`).** Agregação _stateful_ com watermark:
  `groupBy(window(pickup_datetime, "1 day")).count()` — número de corridas por janela
  diária. Provoca shuffle por chave de janela.
- **[WORKLOAD-2] Tempo médio por zona (`avg_duration_by_zone`).** `groupBy(window(1
  dia), pu_location_id).avg(trip_duration_s)` **+ join** com a tabela de zonas para
  enriquecer com borough/zone. Combina shuffle de agregação e de join — a carga mais
  cara.

Estas cargas nomeadas são as avaliadas nos experimentos da seção 6.

## 6. Experiments and results

> Benchmarking com **≥ 3 repetições** por configuração e análise estatística
> (média ± desvio padrão amostral). Harness: [`bin/run_benchmarks.sh`](bin/run_benchmarks.sh);
> dados brutos em [`misc/results/`](misc/results/) (`runs*.csv`, `summary*.csv`, `figs/`).

### 6.1 Experimental environment

**Fase 1 — host único:** CPU 12 núcleos lógicos, 16 GB RAM, Ubuntu Linux, Docker 28 /
Compose v2 (todos os serviços em contêineres numa rede _bridge_).

**Fase 2 — cluster AWS:** 4 VMs EC2 (`us-east-1a`), Docker Swarm + rede overlay
(VXLAN). node-infra (`t3.large`, 2 vCPU): Kafka + Spark master/driver + producer;
node-w1/w2/w3 (`t3.medium`, 2 vCPU cada): 1 Spark executor por VM
(`max_replicas_per_node: 1` ⇒ escala horizontal **real**). Config em
[`misc/docker-stack.yml`](misc/docker-stack.yml).

### 6.2 How to perform benchmarking

**Métricas:** *throughput* (registros/s processados pelo Spark) e *latência de
micro-batch* (ms), extraídas do `StreamingQueryProgress` de cada batch via um
`StreamingQueryListener` (flag `--metrics-file`). **Fatores variados:**

- **Cenário A (escala vertical):** `spark.executor.cores` ∈ {1, 2, 4}, workers fixo.
- **Cenário B (escala horizontal):** nº de workers ∈ {1, 2, 3}, cores fixo em 4.

Cada configuração é repetida **3×** (`REPS=3`), com volume fixo de mensagens por run
e estado limpo (tópico recriado, `output/`/`checkpoints/` zerados) entre runs.

```bash
cd g11
# validação rápida com a amostra:
MAX_RECORDS=20000 REPS=3 SCENARIOS="A" bin/run_benchmarks.sh
column -t -s, < misc/results/summary.csv
# experimento completo (dataset grande): DATA_DIR_HOST=../data MAX_RECORDS=2000000 bin/run_benchmarks.sh
```

### 6.3 What did you test?

Variamos **cores por executor** (A) e **número de workers** (B); medimos **throughput**
e **latência**, cada configuração com **3 repetições**, reportando média ± desvio.

### 6.4 Results

#### Fase 1 — host único (12 cores), 3 repetições, 2M msgs/run

| Cenário | Config | Throughput médio (reg/s) | Std | Latência média (ms) | Std | Runs |
|---|---|---:|---:|---:|---:|:-:|
| A (cores) | 1 core  | 23 424,5 | 471,4 | 42 701,8 | 857,1 | 3 |
| A (cores) | 2 cores | 32 544,6 | 472,9 | 30 731,3 | 443,2 | 3 |
| A (cores) | 4 cores | 46 333,6 |  80,6 | 21 582,7 |  37,5 | 3 |
| B (workers) | 1 worker  | 46 084,2 | 1 349,1 | 21 712,0 | 645,5 | 3 |
| B (workers) | 2 workers | 46 508,3 |   523,5 | 21 503,3 | 241,4 | 3 |
| B (workers) | 3 workers | 42 648,4 |   182,4 | 23 447,8 | 100,0 | 3 |

**Discussão:** No host único, a **escala vertical funciona** (1→4 cores ≈ **+98 %** de
throughput), mas a **escala horizontal satura**: adicionar workers de 1→3 **não
acelera** (chega a piorar em 3, por overhead de coordenação num só host de 12 cores).
Desvios baixos (< 3 % da média) indicam medições estáveis.

#### Fase 2 — cluster AWS (4 VMs), 3 repetições

| Cenário | Config | Throughput médio (reg/s) | Std | Latência média (ms) | Std | Runs |
|---|---|---:|---:|---:|---:|:-:|
| A (cores) | 1 core  | 19 965,5 |   990,9 | 50 167,3 | 2 443,6 | 3 |
| A (cores) | 2 cores | 24 915,4 |   260,2 | 40 138,7 |   420,8 | 3 |
| B (workers) | 1 worker  | 17 435,6 | 1 864,5 | 59 712,2 | 6 589,8 | 3 |
| B (workers) | 2 workers | 26 783,1 | 1 430,5 | 37 410,0 | 2 050,4 | 3 |
| B (workers) | 3 workers | 24 454,5 |   303,5 | 40 896,5 |   511,0 | 3 |

**Discussão / achado central:** No cluster AWS, com **cada worker numa VM física
distinta**, a escala horizontal **realmente escala**: 1→2 workers = **+54 %** de
throughput (17 435 → 26 783 reg/s). O ganho satura em 3 workers — teto de um cluster
pequeno de instâncias _burstable_ e da chave de particionamento concentrada em 4
valores (§8.4 do relatório). O AWS é mais lento em absoluto que o host de 12 cores
(VMs de 2 vCPU + latência de rede real no shuffle), mas é onde a distribuição
**demonstra seu valor** — exatamente o que o host único não conseguia mostrar.

Gráficos com barras de erro em [`misc/results/figs/`](misc/results/figs/) (ex.:
`painel_completo.png`, `compare_B_throughput.png`).

## 7. Limitations and conclusions

**O que funcionou:** o pipeline stream (Kafka + Spark) roda out-of-the-box só com
Docker, ingere e agrega os dados, e o desenho experimental demonstra o ponto central —
**escala horizontal só entrega quando há máquinas físicas distintas** (host único
satura; cluster AWS escala +54 % de 1→2 workers).

**Limitações:** *(i)* host único (12 cores) simula "horizontal" com processos
concorrentes; *(ii)* cluster AWS pequeno (4 VMs) e instâncias `t3` _burstable_ (sujeitas
a _throttling_); *(iii)* chave de particionamento `service_type` tem só 4 valores,
concentrando o shuffle e limitando o paralelismo; *(iv)* subconjunto de ≈48,8M linhas,
não os ≈102M completos; *(v)* a latência medida é de processamento de micro-batch, não
end-to-end por evento. Trabalhos futuros: cluster maior de CPU dedicada, rebalancear a
chave de particionamento, semântica _exactly-once_, e medir latência end-to-end. Detalhes
em [`RELATORIO.md`](RELATORIO.md) §8–§9.

## 8. References and external resources

- **NYC TLC Trip Record Data** — <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- **Apache Kafka** (KRaft) — <https://kafka.apache.org/>
- **Apache Spark — Structured Streaming** —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html>
- **Conector Spark-Kafka** — `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4`
- **Docker / Docker Compose / Docker Swarm** — <https://docs.docker.com/>
- Imagens: `apache/kafka:3.9.0`, `bitnamilegacy/spark:3.5.4`
- Bibliotecas Python (producer): `confluent-kafka`, `pandas`, `pyarrow`
- Relatório técnico completo: [`RELATORIO.md`](RELATORIO.md)
