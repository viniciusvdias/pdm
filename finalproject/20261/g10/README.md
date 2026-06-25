<div align="center">

🇺🇸 English &nbsp;|&nbsp; 🇧🇷 [Português](README.pt-br.md)

</div>

---

# Final project report: Wikimedia Real-Time Topic Analytics

## 1. Context and motivation

Wikipedia is edited continuously by millions of contributors worldwide. The Wikimedia Foundation exposes every one of those edits as a public real-time event stream, producing roughly **300 events per second**. This project taps into that stream and answers a simple question: *which topics are people editing right now — and which ones are growing fastest?*

To do so we combine three Big Data challenges at once:

- **Volume and velocity**: hundreds of events per second, 24/7, with no upper bound.
- **Heterogeneity**: titles arrive in every language; some are article names, others are cryptic Wikidata item IDs.
- **Slow operator in a fast pipeline**: semantic classification using a sentence-transformer model takes ~10–50 ms per batch while the stream itself advances every millisecond, forcing us to solve the "fast-path / slow-path" problem that characterises real Big Data systems.

The system classifies each edit into one of 12 pre-defined topics (Sports, Technology, Politics, …), aggregates counts over 5-minute windows, and exposes the results through a live dashboard.

---

## 2. Data

### 2.1 Detailed description

**Source**: [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) — a public Server-Sent Events (SSE) endpoint.

**Endpoint used**:
```
https://stream.wikimedia.org/v2/stream/recentchange
```

Each event is a JSON object. The fields we consume are:

| Field          | Type    | Description                                         |
|----------------|---------|-----------------------------------------------------|
| `title`        | string  | Article or page name as it appears in Wikipedia     |
| `namespace`    | integer | 0 = article, 6 = File, 14 = Category, 146 = Wikidata item… |
| `comment`      | string  | Raw edit summary entered by the contributor         |
| `parsedcomment`| string  | Edit summary with wiki-links resolved to HTML       |
| `timestamp`    | integer | Unix timestamp of the edit (seconds)                |

Example event (truncated):
```json
{
  "title": "Cristiano Ronaldo",
  "namespace": 0,
  "comment": "/* Goals */ added 2026 stats",
  "parsedcomment": "/* <a href=\"...\">Goals</a> */ added 2026 stats",
  "timestamp": 1781970587
}
```

**Volume**: ~300 raw events/s globally; after filtering to type `edit` and namespace 0/6/14/146, roughly 80–150 events/s reach the classifier.

### 2.2 How to obtain the data

A small sample is already included in [`datasample/wikimedia-recentchange-sample.txt`](datasample/wikimedia-recentchange-sample.txt) (raw SSE lines, < 1 MB). It is used automatically during quick-start.

The full dataset is the **live stream itself** — no download is needed. The producer container connects to the Wikimedia endpoint at startup and streams events continuously. An active internet connection is required.

---

## 3. How to install and run

> **The only requirement is Docker with Docker Compose.**  
> No Python, Spark, or Kafka installation is needed on the host.

### GPU vs CPU

| Hardware | Configuration |
|---|---|
| **NVIDIA RTX 5060 (8 GB VRAM)** — recommended | No changes needed. Docker uses the GPU automatically via the NVIDIA Container Toolkit. |
| **CPU only (≥ 32 GB RAM)** | Remove the `deploy.resources.reservations.devices` block from `docker-compose.yaml` before running. The model and tensors fit comfortably in 32 GB RAM. |

### 3.1 Quick start (using sample data in `datasample/`)

```bash
# From the finalproject/20261/g10 directory:
./bin/start.sh
```

Or manually:

```bash
docker compose up --build
```

Once all containers are healthy, open the dashboard at **http://localhost:8501**.

Results appear after the first 5-minute window completes. You can watch progress in the Spark logs:

```bash
docker logs -f spark_consumer
```

### 3.2 How to run with the full dataset

The default configuration already points to the live Wikimedia stream (`WIKIMEDIA_URL` in `.env`). No additional setup is needed — the producer connects automatically.

To change the classification model, edit `.env`:

```bash
# Multilingual (default) — covers all Wikipedia languages:
MODEL_NAME=intfloat/multilingual-e5-base

# English-only, ~3x faster — useful for benchmarks:
MODEL_NAME=all-MiniLM-L6-v2
```

To change the aggregation window:
```bash
WINDOW_DURATION_NUMERIC=5
WINDOW_DURATION_UNIT=minutes
```

---

## 4. Project architecture

```
[Wikimedia SSE API]
  https://stream.wikimedia.org/v2/stream/recentchange
           |
           | HTTP SSE  (~300 events/s)
           v
[Producer]  (src/producer/main.py)
  kafka-python — parses SSE lines, extracts title/timestamp/namespace/comment
           |
           | JSON  →  topic: wikimediaRecentchange  (3 partitions)
           v
[Apache Kafka 3.7]  (KRaft mode, no ZooKeeper)
           |
           | Kafka consumer
           v
[Spark Consumer]  (src/spark_consumer/)
  spark-submit main.py  (PySpark 3.5.1, local[*])
  |
  +-- WORKLOAD-2  Text Cleaning
  |     Strip wiki-links [[target|text]], section comments /* ... */,
  |     namespace prefixes (File:, Category:), HTML from parsedcomment.
  |     Wikidata Q-items use parsedcomment as the classification text.
  |
  +-- WORKLOAD-1  Semantic Classification  ←  classifier.py
  |     Pandas UDF (Apache Arrow): entire micro-batch column → single
  |     model.encode() call → cosine similarity vs. 12 category vectors
  |     → argmax → raw string "Category|confidence" (e.g. "Sports|0.93")
  |     → split into columns: category (string) + confidence (float)
  |     GPU: any NVIDIA GPU with CUDA 12.8 support (optional)
  |     CPU: fallback, requires >= 32 GB RAM
  |
  +-- WORKLOAD-3  Windowed Aggregation
        window(event_time, 5 min) + watermark(10 min)
        GROUP BY (window, category)  COUNT(*)
           |
           | upsert  UNIQUE(window_start, window_end, category)
           v
[SQLite]  (WAL mode)  — /data/analytics.sqlite3
  tables: events, window_metrics
           |
           | SQL queries  (polled every 30 s)
           v
[Dashboard]  (src/dashboard/main.py)
  Streamlit  →  http://localhost:8501
  - KPI strip: total edits, top category, active categories, window label
  - Bar chart + donut: edits per category, current window
  - Popularity score: relative activity within the window (100 = most edited)
  - Historical line chart: category counts over the last 24 windows
  - Recent edits table: last 50 individual classified events
```

### Component summary

| Container | Image / Base | Role |
|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | Message broker (KRaft, no ZooKeeper) |
| `wikimedia_producer` | `python:3.11-slim` | SSE consumer → Kafka producer |
| `spark_consumer` | `apache/spark:3.5.1` + Python 3.9 | Classification + aggregation |
| `sqlite_init` | same as spark_consumer | Schema initialisation (runs once) |
| `wikimedia_dashboard` | `python:3.11-slim` | Streamlit dashboard |

All persistent data is stored in Docker named volumes (`kafka_data`, `spark_checkpoint`, `sqlite_data`).

---

## 5. Workloads evaluated

### [WORKLOAD-1] Semantic classification

**What it does**: Each cleaned article title is embedded by a sentence-transformer model and compared via cosine similarity against 12 pre-computed category vectors. The category with the highest similarity score is assigned.

**Implementation**: `classifier.py` — a Spark Pandas UDF backed by `sentence-transformers`. The UDF receives the entire `cleaned_text` column of a micro-batch as a `pandas.Series` and performs one batched `model.encode()` call, minimising Python↔JVM round-trips.

**Output columns**: the UDF returns a raw `"Category|confidence"` string, which `main.py` immediately splits into two columns — `category` (string, used in `groupBy`) and `confidence` (float, stored per event). The `groupBy` never sees the combined string.

**"Others" fallback**: if the cleaned text is empty or whitespace-only, the event is assigned to `"Others"` with confidence `0.0` without hitting the model. `"Others"` is intentional but is not part of the taxonomy in `topics.json` — it exists only as an internal fallback. The dashboard has a colour mapped to it (`#bdc3c7`).

**Why it is the hard workload**: embedding inference is the computational bottleneck. The model runs on GPU (NVIDIA with CUDA 12.8) or CPU (≥ 32 GB RAM), and batch size, model size, and number of categories all affect throughput.

---

### [WORKLOAD-2] Text preprocessing

**What it does**: Normalises Wikimedia events before classification:

- Resolves wiki-links `[[target|text]]` → `text`
- Removes section markers `/* ... */` from edit comments
- Strips namespace prefixes (`File:`, `Category:`, `Categoria:`) for namespaces 6 and 14
- For Wikidata items (titles matching `^Q\d+`): uses `parsedcomment` with HTML tags stripped instead of the bare Q-number, which carries no semantic content

**Implementation**: `clean_text_column()` in `main.py`, composed of Spark SQL functions (no Python UDF, runs fully JVM-side).

---

### [WORKLOAD-3] Temporal windowed aggregation

**What it does**: Groups classified events by `(category, 5-min tumbling window)`, counts occurrences, and computes a relative popularity score (`count / max_count * 100`). Results are upserted into SQLite.

**Implementation**: `df_windowed` in `main.py` — Spark Structured Streaming `window()` with a 10-minute watermark for late-arrival handling. The `foreachBatch` sink writes only the most recent completed window per micro-batch.

---

## 6. Experiments and results

### 6.1 Experimental environment

Experiments were run on a consumer laptop under Docker Desktop (Windows 11 Pro), **CPU-only** (no GPU). The English-only model `all-MiniLM-L6-v2` was used in all benchmarks; the multilingual model (`intfloat/multilingual-e5-base`) was excluded because inference on CPU-only hardware is prohibitively slow for iterative benchmarking.

| Component | Specification |
|---|---|
| OS | Windows 11 Pro (build 26200) |
| CPU | Intel, 16 GB RAM — no GPU |
| Docker | Docker Desktop (latest stable) |
| Spark mode | `local[*]` (single container) |
| Kafka partitions | 3 |
| Model (all experiments) | `all-MiniLM-L6-v2` |
| Window (Exp 1, 2, 4) | 1 minute (faster iteration) |
| Repetitions per config | 1 (see note below) |

> **Note on repetitions**: each configuration requires ~3.5 minutes of stabilisation before metrics can be collected (one full window + container warm-up). The full benchmark suite runs for ~1.5 hours in a single pass. Single-run results give clear directional trends; additional repetitions are recommended for a production benchmark.

### 6.2 How to perform benchmarking

Each experiment is run by changing one variable in `.env`, stopping and recreating only the `spark_consumer` container (Kafka and the producer keep running), and collecting `docker stats` metrics during a 60-second steady-state window after the first full window completes.

Metrics collected:
- **CPU / Memory**: `docker stats spark_consumer --no-stream` polled every 5 seconds, averaged over 60 seconds
- **Throughput**: derived from SQLite (`sum(count) / window_duration_seconds` for the most recent completed window)

The full benchmark suite (all 4 experiments, 11 configurations) is automated in `benchmark.ps1` at the project root. It runs unattended in ~1.5 hours and writes timestamped results to `benchmark_results.txt`.

```powershell
# Run the full benchmark suite (requires containers already running):
powershell -File benchmark.ps1
```

Due to the time cost per configuration (~5 minutes of stabilisation + 1 minute of sampling), each configuration was run **once**. Results are directional; standard deviation is not available.

### 6.3 What did you test?

#### Experiment 1 — Impact of semantic embeddings (WORKLOAD-1)

Compared the full embedding pipeline (`CLASSIFIER_MODE=embedding`) against running with no classifier (`CLASSIFIER_MODE=none`, all events labelled "Others"). The keyword-matching baseline and multilingual-e5 variant were not tested in this environment (CPU-only constraint).

| Variable | Values tested |
|---|---|
| Pipeline variant | `embedding` (all-MiniLM-L6-v2) vs. `none` (no model) |
| Metrics | CPU usage (%), RAM usage (MB), throughput (ev/s) |

#### Experiment 2 — Spark local parallelism

Varied `SPARK_MASTER` to control the number of local executor threads. All other parameters held constant (`CLASSIFIER_MODE=embedding`, 1-minute window, 12-category taxonomy).

| Variable | Values tested |
|---|---|
| `SPARK_MASTER` | `local[1]`, `local[2]`, `local[4]` |
| Metrics | CPU usage (%), RAM usage (MB) |

#### Experiment 3 — Aggregation window size

Varied `WINDOW_DURATION` to measure state memory growth and CPU behaviour across window sizes.

| Variable | Values tested |
|---|---|
| Window duration | 1 min, 5 min, 10 min |
| Metrics | CPU usage (%), RAM usage (MB) |

#### Experiment 4 — Number of categories (WORKLOAD-1)

Varied the number of topic categories in the taxonomy file, keeping all other parameters constant.

| Variable | Values tested |
|---|---|
| Category count | 5 (`topics_5.json`), 10 (`topics_10.json`), 12 (`topics.json`) |
| Metrics | CPU usage (%), RAM usage (MB) |

### 6.4 Results

#### Functional and integration tests

Before benchmarking, the pipeline was validated with 7 functional tests and 2 integration tests. All 9 passed.

| ID | Description | Result |
|---|---|---|
| F1 | Producer receives events from Wikimedia (multi-language, all namespaces) | ✅ Pass |
| F2 | Kafka receives all events with correct JSON schema (`title`, `namespace`, `comment`, `timestamp`) | ✅ Pass |
| F3 | Spark Structured Streaming processes 2 complete 5-minute windows without errors | ✅ Pass |
| F4 | Classifier accuracy: 26/30 known titles correctly labelled — **86.7%** (target: 80%) | ✅ Pass |
| F5 | Windowed aggregation: 8,835 events across 12 categories in window 00:05–00:10 | ✅ Pass |
| F6 | Growth detection: Military ×16.3, Entertainment ×10.2 correctly identified between windows | ✅ Pass |
| F7 | SQLite: 24 rows in `window_metrics`, 315 in `events`; no duplicates (upsert verified) | ✅ Pass |
| I1 | End-to-end trace: Cyrillic title originated on Wikimedia → JSON in Kafka → classified by Spark → persisted in SQLite → visible in dashboard | ✅ Pass |
| I2 | Recovery: `spark_consumer` restarted; back to healthy in **8 seconds**, checkpoint preserved, no data lost | ✅ Pass |

**Classifier accuracy detail** — 4 misclassified titles (out of 30):

| Title | Expected | Got | Reason |
|---|---|---|---|
| Formula 1 | Sports | Science | Title alone resembles a chemistry formula |
| World War II | History | Military | Genuine semantic overlap — both are defensible |
| William Shakespeare | Arts | Entertainment | Theatre maps to entertainment in the embedding space |
| European Union | Politics | Business | Strong economic connotation in the model |

---

#### Experiment 1 — Embedding impact

1-minute windows; CPU and RAM averaged over a 60-second steady-state sampling window per configuration.

| Configuration | CPU (%) | RAM (MB) | Throughput (ev/s) | Runs |
|---|---|---|---|---|
| `embedding` — all-MiniLM-L6-v2 | 209.4 | 1,995 | ~29 | 1 |
| `none` — no classifier | 440.3 | 1,994 | ~29 | 1 |

**Discussion**: removing the embedding model *increases* CPU usage by 110%. With the model active, inference (~10–50 ms/batch) paces the pipeline, leaving Spark partially idle between batches. Without the model, Spark runs at full speed and saturates all threads with Kafka polling, aggregation, and SQLite writes. Throughput is identical (~29 ev/s) in both cases because the source rate is fixed by the Wikimedia stream — the system is input-bound, not compute-bound.

---

#### Experiment 4 — Number of categories

All runs: 1-minute windows, `CLASSIFIER_MODE=embedding`, `SPARK_MASTER=local[*]`.

| Categories | Taxonomy file | CPU (%) | RAM (MB) | Runs |
|---|---|---|---|---|
| 5 | `topics_5.json` | 340.1 | 2,051 | 1 |
| 10 | `topics_10.json` | 239.4 | 1,385 | 1 |
| 12 | `topics.json` (default) | 209.4 | 1,995 | 1 |

**Discussion**: more categories reduce CPU usage — the inverse of what cosine-similarity cost alone would predict. With fewer categories, events concentrate into larger groups, increasing Spark's per-partition aggregation cost and the size of each SQLite write batch. The O(N × embedding\_dim) similarity step is negligible relative to these I/O costs. The default 12-category taxonomy achieves the lowest CPU usage of the three configurations tested.

---

#### Experiment 2 — Spark local parallelism

All runs: 1-minute windows, `CLASSIFIER_MODE=embedding`, 12-category taxonomy.

| `SPARK_MASTER` | CPU (%) | RAM (MB) | Runs |
|---|---|---|---|
| `local[1]` | 103.6 | 1,519 | 1 |
| `local[2]` | 215.7 | 1,993 | 1 |
| `local[4]` | 211.2 | 2,024 | 1 |

**Discussion**: CPU scales linearly from `local[1]` to `local[2]` (~104% → ~216%), showing that the pipeline uses two full cores effectively when they are available. However, going from `local[2]` to `local[4]` produces no measurable gain (215.7% → 211.2%). This is the clearest confirmation that the system is **input-bound at ~29 ev/s**: the Wikimedia stream cannot feed events fast enough to keep 4 threads busy. Two threads is the point of saturation for this input rate; beyond that, additional threads add JVM scheduling overhead with no throughput benefit.

---

#### Experiment 3 — Aggregation window size

All runs: `CLASSIFIER_MODE=embedding`, `SPARK_MASTER=local[*]`, 12-category taxonomy.

| Window duration | CPU (%) | RAM (MB) | Runs |
|---|---|---|---|
| 1 min | 206.7 | 1,961 | 1 |
| 5 min | 212.9 | 2,020 | 1 |
| 10 min | 210.8 | 2,628 | 1 |

**Discussion**: CPU is nearly constant across all window sizes (~207–213%), which confirms that the classification step (WORKLOAD-1) dominates CPU cost and runs at the same rate regardless of how long Spark accumulates state. Window size only affects how many events are held in the aggregation state before a flush. RAM tells the real story: the 10-minute window requires 34% more memory than the 1-minute window (+667 MB), driven by the larger watermark state Spark must maintain to handle late arrivals. For production use, a 5-minute window offers the best balance: negligible RAM overhead over the 1-minute baseline (+59 MB) with a useful granularity for trend detection.

---

#### Live data snapshot — 5-minute production window

Captured during functional testing, window 00:05–00:10 UTC, 5-minute tumbling window.

| Category | Events | Category Share (%) | Growth vs. prev. window |
|---|---|---|---|
| Politics | 2,195 | 24.8 | ×5.4 |
| History | 1,233 | 14.0 | ×4.6 |
| Health | 1,130 | 12.8 | ×5.0 |
| Geography | 1,041 | 11.8 | ×4.9 |
| Arts | 961 | 10.9 | ×6.1 |
| Technology | 657 | 7.4 | ×3.6 |
| Sports | 354 | 4.0 | ×6.9 |
| Science | 351 | 4.0 | ×2.8 |
| Entertainment | 317 | 3.6 | ×10.2 |
| Business | 235 | 2.7 | ×5.2 |
| Military | 195 | 2.2 | ×16.3 |
| Religion | 166 | 1.9 | ×3.3 |
| **Total** | **8,835** | 100 | ×5.0 |

Politics is consistently the most-edited category. Military showed the highest growth between windows (×16.3), consistent with live-event coverage spikes during the collection period.

> **Metric note**: "Category Share" (`count / max_count × 100`) measures relative volume within a single window — it identifies the dominant category, not the fastest-growing one. Cross-window growth is captured by the "Growth" column above. The two metrics answer different questions and should not be conflated.

---

## 7. Limitations and conclusions

### What worked

The pipeline is fully functional end-to-end. All 7 functional tests and 2 integration tests passed without failure. The classifier reached 86.7% accuracy with `all-MiniLM-L6-v2`, above the 80% target. Recovery after a container restart takes 8 seconds with no data loss, thanks to Spark checkpointing and Kafka offset retention.

All 4 benchmark experiments produced clear, consistent trends:

- **Embedding as pacer (Exp 1)**: the classifier slows Spark to a sustainable rate, paradoxically halving CPU usage compared to running with no model. The system is input-bound (~29 ev/s from the Wikimedia stream), not compute-bound.
- **More categories = lower CPU (Exp 4)**: aggregation cost dominates, not cosine-similarity cost. Spreading events across more groups reduces per-partition work in Spark.
- **Parallelism ceiling at 2 threads (Exp 2)**: local[2] and local[4] consume the same CPU (~213%), confirming the input-bound hypothesis — more threads cannot help if the source cannot feed them faster.
- **Window size affects RAM, not CPU (Exp 3)**: CPU is flat (~208%) across 1/5/10-minute windows; RAM grows +34% at 10 minutes due to watermark state. The 5-minute default is the optimal trade-off.

### Implementation challenges

- **PyTorch inside a Spark Pandas UDF**: the sentence-transformer model cannot be serialized across the JVM↔Python boundary in the usual way. The model must be lazily initialized *inside* the UDF at first call. Getting this right, and avoiding multiple redundant loads per executor, was the trickiest part of the integration.
- **Multilingual content**: Wikimedia events arrive in 300+ languages. The English-only `all-MiniLM-L6-v2` model misclassifies or ignores non-English titles. The intended production model is `intfloat/multilingual-e5-base`, but it is too slow for CPU-only hardware. On GPU, it is the correct choice.
- **Wikidata Q-IDs**: articles in the Wikidata namespace have titles like `Q123456`, which carry no semantic content. The pipeline detects these with a regex and substitutes the `parsedcomment` field (the human-readable edit summary) as the classification text.
- **Kafka listener configuration**: making Kafka reachable both from within Docker (inter-container) and from the host required separate listener names (`PLAINTEXT` vs `PLAINTEXT_HOST`) with different advertised addresses. Misconfiguring this produced silent failures where the producer appeared to send but Spark never received.
- **SQLite concurrent access**: Streamlit polling SQLite while Spark was mid-write caused `database is locked` errors under the default journal mode. Switching to WAL mode resolved this — WAL allows concurrent readers with one writer at no risk of corruption.
- **Event time vs. processing time**: using event timestamps (when the edit happened) rather than ingestion time for `window()` required careful watermark tuning. Too short a watermark drops late events; too long keeps unnecessary state in memory.

### Design decisions

| Decision | Alternative considered | Why we chose this |
|---|---|---|
| **Kafka as buffer** | Direct Spark SSE consumer | Kafka retains messages if Spark restarts; decouples producer rate from consumer speed |
| **SQLite as sink** | PostgreSQL, Redis | Single file, no extra container, WAL mode handles our write rate; sufficient for a single-reader dashboard |
| **Spark local mode** | Distributed YARN/K8s cluster | The sentence-transformer model must be available on every executor; local mode avoids distributing a 90 MB model binary |
| **Sentence-transformers** | Keyword rules, LLM API | Keyword rules fail for non-English titles (60%+ of Wikimedia edits); LLM APIs add latency and cost; embeddings work cross-lingually at inference speed |
| **Pandas UDF for classification** | Row-by-row Python UDF | One batched `model.encode()` call per micro-batch vs. one call per row — orders of magnitude faster due to GPU/CPU vectorisation |

### Limitations

- **Single-node Spark**: the pipeline runs in `local[*]` mode inside one container. Distributed mode (YARN / Kubernetes) would require deploying the sentence-transformer model to all worker nodes, which is non-trivial for large models.
- **SQLite as sink**: SQLite handles our write rate well at 1-second micro-batches, but would become a bottleneck under heavy parallel reads (multiple dashboard instances) or if micro-batch duration drops below ~200 ms.
- **Taxonomy is static**: categories are fixed at startup. Adding or removing a category requires restarting `spark_consumer` so the category embeddings are re-computed.
- **Classification quality**: cosine similarity against a single label string is a strong but imperfect proxy. Titles with genuine semantic ambiguity (e.g. "Mercury" — Science, Geography, or Entertainment?) will always be debatable. Accuracy of 86.7% on a 30-title sample is indicative, not statistically conclusive.
- **Input-bound throughput**: the ~29 ev/s rate is set by the Wikimedia stream, not by the system's capacity. Benchmarking with a controlled synthetic source (e.g. a replay producer at variable rate) would give a cleaner picture of the true processing ceiling.
- **CPU-only environment**: the multilingual model (`intfloat/multilingual-e5-base`) was not benchmarked. On GPU hardware it would be the preferred model for production; on CPU it is too slow for iterative experimentation.
- **Single run per configuration**: each benchmark configuration was run once due to the time cost of each run (~5 minutes per configuration × 11 configurations ≈ 1.5 hours total). Standard deviation cannot be reported; results are directional.

---

## 8. References and external resources

- [Wikimedia EventStreams documentation](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams)
- [Apache Kafka 3.7 documentation](https://kafka.apache.org/37/documentation.html)
- [Apache Spark 3.5 Structured Streaming guide](https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html)
- [sentence-transformers library](https://www.sbert.net/)
- Model: [intfloat/multilingual-e5-base](https://huggingface.co/intfloat/multilingual-e5-base) — Wang et al., 2024
- Model: [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) — Reimers & Gurevych, 2019
- [Streamlit documentation](https://docs.streamlit.io/)
- [PyTorch 2.7 + CUDA 12.8 wheels](https://download.pytorch.org/whl/cu128)

## 1. Contexto e motivação

A Wikipedia é editada continuamente por milhões de colaboradores ao redor do mundo. A Wikimedia Foundation expõe cada uma dessas edições como um stream público de eventos em tempo real, gerando cerca de **300 eventos por segundo**. Este projeto se conecta a esse stream e responde a uma pergunta simples: *quais tópicos as pessoas estão editando agora — e quais estão crescendo mais rápido?*

Para isso, combinamos três desafios de Big Data ao mesmo tempo:

- **Volume e velocidade**: centenas de eventos por segundo, 24/7, sem limite superior.
- **Heterogeneidade**: os títulos chegam em todos os idiomas; alguns são nomes de artigos, outros são IDs crípticos do Wikidata.
- **Operador lento em um pipeline rápido**: a classificação semântica usando um modelo sentence-transformer leva ~10–50 ms por batch, enquanto o stream avança a cada milissegundo — isso nos obriga a resolver o problema "fast-path / slow-path" que caracteriza sistemas reais de Big Data.

O sistema classifica cada edição em um de 12 tópicos pré-definidos (Esportes, Tecnologia, Política, …), agrega contagens em janelas de 5 minutos e exibe os resultados em um dashboard ao vivo.

---

## 2. Dados

### 2.1 Descrição detalhada

**Fonte**: [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) — um endpoint público de Server-Sent Events (SSE).

**Endpoint utilizado**:
```
https://stream.wikimedia.org/v2/stream/recentchange
```

Cada evento é um objeto JSON. Os campos que consumimos são:

| Campo          | Tipo    | Descrição                                                      |
|----------------|---------|----------------------------------------------------------------|
| `title`        | string  | Nome do artigo ou página como aparece na Wikipedia             |
| `namespace`    | inteiro | 0 = artigo, 6 = Arquivo, 14 = Categoria, 146 = item Wikidata… |
| `comment`      | string  | Resumo bruto da edição inserido pelo colaborador               |
| `parsedcomment`| string  | Resumo da edição com wiki-links resolvidos para HTML           |
| `timestamp`    | inteiro | Timestamp Unix da edição (segundos)                            |

Exemplo de evento (truncado):
```json
{
  "title": "Cristiano Ronaldo",
  "namespace": 0,
  "comment": "/* Goals */ added 2026 stats",
  "parsedcomment": "/* <a href=\"...\">Goals</a> */ added 2026 stats",
  "timestamp": 1781970587
}
```

**Volume**: ~300 eventos brutos/s globalmente; após filtrar pelo tipo `edit` e namespaces 0/6/14/146, cerca de 80–150 eventos/s chegam ao classificador.

### 2.2 Como obter os dados

Uma amostra pequena já está incluída em [`datasample/wikimedia-recentchange-sample.txt`](datasample/wikimedia-recentchange-sample.txt) (linhas SSE brutas, < 1 MB). Ela é usada automaticamente no quick start.

O dataset completo é o **stream ao vivo em si** — nenhum download é necessário. O container produtor se conecta ao endpoint da Wikimedia na inicialização e faz o streaming contínuo dos eventos. É necessária uma conexão ativa à internet.

---

## 3. Como instalar e executar

> **O único requisito é Docker com Docker Compose.**  
> Não é necessário instalar Python, Spark ou Kafka no host.

### GPU vs CPU

| Hardware | Configuração |
|---|---|
| **NVIDIA RTX 5060 (8 GB VRAM)** — recomendado | Nenhuma mudança necessária. O Docker usa a GPU automaticamente via NVIDIA Container Toolkit. |
| **Somente CPU (≥ 32 GB RAM)** | Remova o bloco `deploy.resources.reservations.devices` do `docker-compose.yaml` antes de executar. O modelo e os tensores cabem confortavelmente em 32 GB de RAM. |

### 3.1 Quick start (usando os dados de amostra em `datasample/`)

```bash
# A partir do diretório finalproject/20261/g10:
./bin/start.sh
```

Ou manualmente:

```bash
docker compose up --build
```

Após todos os containers estarem saudáveis, abra o dashboard em **http://localhost:8501**.

Os resultados aparecem após a primeira janela de 5 minutos ser concluída. Você pode acompanhar o progresso nos logs do Spark:

```bash
docker logs -f spark_consumer
```

### 3.2 Como executar com o dataset completo

A configuração padrão já aponta para o stream ao vivo da Wikimedia (`WIKIMEDIA_URL` no `.env`). Nenhuma configuração adicional é necessária — o produtor se conecta automaticamente.

Para alterar o modelo de classificação, edite o `.env`:

```bash
# Multilíngue (padrão) — cobre todos os idiomas da Wikipedia:
MODEL_NAME=intfloat/multilingual-e5-base

# Somente inglês, ~3x mais rápido — útil para benchmarks:
MODEL_NAME=all-MiniLM-L6-v2
```

Para alterar a janela de agregação:
```bash
WINDOW_DURATION_NUMERIC=5
WINDOW_DURATION_UNIT=minutes
```

---

## 4. Arquitetura do projeto

```
[API SSE da Wikimedia]
  https://stream.wikimedia.org/v2/stream/recentchange
           |
           | HTTP SSE  (~300 eventos/s)
           v
[Producer]  (src/producer/main.py)
  kafka-python — parseia linhas SSE, extrai title/timestamp/namespace/comment
           |
           | JSON  →  tópico: wikimediaRecentchange  (3 partições)
           v
[Apache Kafka 3.7]  (modo KRaft, sem ZooKeeper)
           |
           | Kafka consumer
           v
[Spark Consumer]  (src/spark_consumer/)
  spark-submit main.py  (PySpark 3.5.1, local[*])
  |
  +-- WORKLOAD-2  Limpeza de Texto
  |     Remove wiki-links [[alvo|texto]], comentários de seção /* ... */,
  |     prefixos de namespace (File:, Category:), HTML do parsedcomment.
  |     Itens Wikidata Q-ID usam parsedcomment como texto de classificação.
  |
  +-- WORKLOAD-1  Classificação Semântica  ←  classifier.py
  |     Pandas UDF (Apache Arrow): coluna inteira do micro-batch →
  |     uma chamada model.encode() → similaridade coseno vs. 12 vetores de
  |     categoria → argmax → string "Categoria|confiança" (ex: "Sports|0.93")
  |     → dividida em colunas: category (string) + confidence (float)
  |     GPU: qualquer NVIDIA com suporte a CUDA 12.8 (opcional)
  |     CPU: fallback, requer >= 32 GB RAM
  |
  +-- WORKLOAD-3  Agregação Temporal com Janelas
        window(event_time, 5 min) + watermark(10 min)
        GROUP BY (janela, categoria)  COUNT(*)
           |
           | upsert  UNIQUE(window_start, window_end, category)
           v
[SQLite]  (modo WAL)  — /data/analytics.sqlite3
  tabelas: events, window_metrics
           |
           | consultas SQL  (polling a cada 30 s)
           v
[Dashboard]  (src/dashboard/main.py)
  Streamlit  →  http://localhost:8501
  - KPIs: total de edições, categoria líder, categorias ativas, janela atual
  - Gráfico de barras + donut: edições por categoria na janela atual
  - Pontuação de popularidade: atividade relativa na janela (100 = mais editada)
  - Gráfico histórico: contagens por categoria nas últimas 24 janelas
  - Tabela de edições recentes: últimos 50 eventos classificados individualmente
```

### Resumo dos componentes

| Container | Imagem / Base | Função |
|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | Broker de mensagens (KRaft, sem ZooKeeper) |
| `wikimedia_producer` | `python:3.11-slim` | Consumidor SSE → produtor Kafka |
| `spark_consumer` | `apache/spark:3.5.1` + Python 3.9 | Classificação + agregação |
| `sqlite_init` | mesmo que spark_consumer | Inicialização do schema (executa uma vez) |
| `wikimedia_dashboard` | `python:3.11-slim` | Dashboard Streamlit |

Todos os dados persistentes são armazenados em volumes Docker nomeados (`kafka_data`, `spark_checkpoint`, `sqlite_data`).

---

## 5. Cargas de trabalho avaliadas

### [WORKLOAD-1] Classificação semântica

**O que faz**: cada título de artigo limpo é transformado em embedding por um modelo sentence-transformer e comparado via similaridade coseno contra 12 vetores de categoria pré-computados. A categoria com maior pontuação de similaridade é atribuída.

**Implementação**: `classifier.py` — uma Pandas UDF do Spark com `sentence-transformers`. A UDF recebe toda a coluna `cleaned_text` de um micro-batch como `pandas.Series` e realiza uma única chamada `model.encode()` em batch, minimizando round-trips Python↔JVM.

**Colunas de saída**: a UDF retorna uma string bruta `"Categoria|confiança"`, que o `main.py` imediatamente divide em duas colunas — `category` (string, usada no `groupBy`) e `confidence` (float, armazenado por evento).

**Fallback "Others"**: se o texto limpo estiver vazio ou for apenas espaços em branco, o evento recebe a categoria `"Others"` com confiança `0.0` sem acionar o modelo. `"Others"` é intencional, mas não faz parte da taxonomia em `topics.json` — existe apenas como fallback interno.

**Por que é a carga de trabalho difícil**: a inferência de embedding é o gargalo computacional. O modelo roda em GPU (NVIDIA com CUDA 12.8) ou CPU (≥ 32 GB RAM), e o tamanho do batch, o tamanho do modelo e o número de categorias afetam o throughput.

---

### [WORKLOAD-2] Pré-processamento de texto

**O que faz**: normaliza os eventos da Wikimedia antes da classificação:

- Resolve wiki-links `[[alvo|texto]]` → `texto`
- Remove marcadores de seção `/* ... */` dos comentários de edição
- Remove prefixos de namespace (`File:`, `Category:`, `Categoria:`) para namespaces 6 e 14
- Para itens Wikidata (títulos que correspondem a `^Q\d+`): usa `parsedcomment` com tags HTML removidas em vez do número Q puro, que não tem conteúdo semântico

**Implementação**: `clean_text_column()` em `main.py`, composta por funções SQL do Spark (sem Python UDF, roda inteiramente no lado JVM).

---

### [WORKLOAD-3] Agregação temporal com janelas

**O que faz**: agrupa eventos classificados por `(categoria, janela tumbling de 5 min)`, conta ocorrências e realiza upsert no SQLite.

**Implementação**: `df_windowed` em `main.py` — `window()` do Spark Structured Streaming com watermark de 10 minutos para tratamento de eventos atrasados. O sink `foreachBatch` escreve apenas a janela completa mais recente por micro-batch.

---

## 6. Experimentos e resultados

### 6.1 Ambiente experimental

Os experimentos foram executados em um notebook pessoal com Docker Desktop (Windows 11 Pro), **somente CPU** (sem GPU). O modelo `all-MiniLM-L6-v2` (somente inglês) foi usado em todos os benchmarks; o modelo multilíngue (`intfloat/multilingual-e5-base`) foi excluído por ser muito lento em hardware somente CPU.

| Componente | Especificação |
|---|---|
| SO | Windows 11 Pro (build 26200) |
| CPU | Intel, 16 GB RAM — sem GPU |
| Docker | Docker Desktop (versão mais recente estável) |
| Modo Spark | `local[*]` (container único) |
| Partições Kafka | 3 |
| Modelo (todos os experimentos) | `all-MiniLM-L6-v2` |
| Janela (Exp 1, 2, 4) | 1 minuto (iteração mais rápida) |
| Repetições por config | 1 (ver nota abaixo) |

> **Nota sobre repetições**: cada configuração requer ~3,5 minutos de estabilização antes de coletar métricas (uma janela completa + aquecimento do container). A suíte completa de benchmark leva ~1,5 hora em uma única passagem. Uma execução por configuração dá tendências direcionais claras; repetições adicionais são recomendadas para um benchmark de produção.

### 6.2 Como realizar o benchmarking

Cada experimento é executado alterando uma variável no `.env`, parando e recriando apenas o container `spark_consumer` (Kafka e o produtor continuam rodando), e coletando métricas do `docker stats` durante uma janela de estado estável de 60 segundos após a primeira janela completa.

Métricas coletadas:
- **CPU / Memória**: `docker stats spark_consumer --no-stream` coletado a cada 5 segundos, com média ao longo de 60 segundos
- **Throughput**: derivado do SQLite (`sum(count) / duração_da_janela_em_segundos` para a janela completa mais recente)

A suíte completa de benchmark (4 experimentos, 11 configurações) está automatizada em `benchmark.ps1` na raiz do projeto. Executa sem supervisão em ~1,5 hora e grava resultados com timestamp em `benchmark_results.txt`.

```powershell
# Executar a suíte completa de benchmark (requer containers já rodando):
powershell -File benchmark.ps1
```

Devido ao custo de tempo por configuração (~5 minutos de estabilização + 1 minuto de amostragem), cada configuração foi executada **uma vez**. Os resultados são direcionais; desvio padrão não está disponível.

### 6.3 O que foi testado?

#### Experimento 1 — Impacto dos embeddings semânticos (WORKLOAD-1)

Comparou o pipeline completo com embedding (`CLASSIFIER_MODE=embedding`) contra a execução sem classificador (`CLASSIFIER_MODE=none`, todos os eventos rotulados como "Others").

| Variável | Valores testados |
|---|---|
| Variante do pipeline | `embedding` (all-MiniLM-L6-v2) vs. `none` (sem modelo) |
| Métricas | Uso de CPU (%), uso de RAM (MB), throughput (ev/s) |

#### Experimento 2 — Paralelismo local do Spark

Variou `SPARK_MASTER` para controlar o número de threads do executor local. Todos os outros parâmetros mantidos constantes.

| Variável | Valores testados |
|---|---|
| `SPARK_MASTER` | `local[1]`, `local[2]`, `local[4]` |
| Métricas | Uso de CPU (%), uso de RAM (MB) |

#### Experimento 3 — Tamanho da janela de agregação

Variou `WINDOW_DURATION` para medir o crescimento do estado em memória e o comportamento da CPU em diferentes tamanhos de janela.

| Variável | Valores testados |
|---|---|
| Duração da janela | 1 min, 5 min, 10 min |
| Métricas | Uso de CPU (%), uso de RAM (MB) |

#### Experimento 4 — Número de categorias (WORKLOAD-1)

Variou o número de categorias de tópicos no arquivo de taxonomia, mantendo todos os outros parâmetros constantes.

| Variável | Valores testados |
|---|---|
| Número de categorias | 5 (`topics_5.json`), 10 (`topics_10.json`), 12 (`topics.json`) |
| Métricas | Uso de CPU (%), uso de RAM (MB) |

### 6.4 Resultados

#### Testes funcionais e de integração

Antes do benchmarking, o pipeline foi validado com 7 testes funcionais e 2 testes de integração. Todos os 9 passaram.

| ID | Descrição | Resultado |
|---|---|---|
| F1 | Produtor recebe eventos da Wikimedia (multi-idioma, todos os namespaces) | ✅ OK |
| F2 | Kafka recebe todos os eventos com schema JSON correto (`title`, `namespace`, `comment`, `timestamp`) | ✅ OK |
| F3 | Spark Structured Streaming processa 2 janelas completas de 5 minutos sem erros | ✅ OK |
| F4 | Acurácia do classificador: 26/30 títulos conhecidos corretamente rotulados — **86,7%** (meta: 80%) | ✅ OK |
| F5 | Agregação com janelas: 8.835 eventos em 12 categorias na janela 00:05–00:10 | ✅ OK |
| F6 | Detecção de crescimento: Military ×16,3, Entertainment ×10,2 corretamente identificados entre janelas | ✅ OK |
| F7 | SQLite: 24 linhas em `window_metrics`, 315 em `events`; sem duplicatas (upsert verificado) | ✅ OK |
| I1 | Rastreamento ponta a ponta: título cirílico originado na Wikimedia → JSON no Kafka → classificado pelo Spark → persistido no SQLite → visível no dashboard | ✅ OK |
| I2 | Recuperação: `spark_consumer` reiniciado; de volta a saudável em **8 segundos**, checkpoint preservado, nenhum dado perdido | ✅ OK |

**Detalhe da acurácia** — 4 títulos classificados incorretamente (de 30):

| Título | Esperado | Obtido | Motivo |
|---|---|---|---|
| Formula 1 | Sports | Science | O título sozinho lembra uma fórmula química |
| World War II | History | Military | Sobreposição semântica genuína — ambos são defensáveis |
| William Shakespeare | Arts | Entertainment | Teatro mapeia para entretenimento no espaço de embedding |
| European Union | Politics | Business | Forte conotação econômica no modelo |

---

#### Experimento 1 — Impacto do embedding

Janelas de 1 minuto; CPU e RAM calculados como média de uma janela de amostragem de 60 segundos em estado estável.

| Configuração | CPU (%) | RAM (MB) | Throughput (ev/s) | Execuções |
|---|---|---|---|---|
| `embedding` — all-MiniLM-L6-v2 | 209,4 | 1.995 | ~29 | 1 |
| `none` — sem classificador | 440,3 | 1.994 | ~29 | 1 |

**Discussão**: remover o modelo de embedding *aumenta* o uso de CPU em 110%. Com o modelo ativo, a inferência (~10–50 ms/batch) cadencia o pipeline, deixando o Spark parcialmente ocioso entre batches. Sem o modelo, o Spark roda em velocidade máxima e satura todas as threads com polling do Kafka, agregação e escritas no SQLite. O throughput é idêntico (~29 ev/s) em ambos os casos porque a taxa da fonte é fixada pelo stream da Wikimedia — o sistema é limitado pela entrada, não pelo processamento.

---

#### Experimento 4 — Número de categorias

Todas as execuções: janelas de 1 minuto, `CLASSIFIER_MODE=embedding`, `SPARK_MASTER=local[*]`.

| Categorias | Arquivo de taxonomia | CPU (%) | RAM (MB) | Execuções |
|---|---|---|---|---|
| 5 | `topics_5.json` | 340,1 | 2.051 | 1 |
| 10 | `topics_10.json` | 239,4 | 1.385 | 1 |
| 12 | `topics.json` (padrão) | 209,4 | 1.995 | 1 |

**Discussão**: mais categorias reduzem o uso de CPU — o inverso do que o custo de similaridade coseno sozinho preveria. Com menos categorias, os eventos se concentram em grupos maiores, aumentando o custo de agregação por partição no Spark e o tamanho de cada batch de escrita no SQLite. O passo de similaridade O(N × embedding_dim) é insignificante em relação a esses custos de I/O. A taxonomia padrão de 12 categorias alcança o menor uso de CPU entre as três configurações testadas.

---

#### Experimento 2 — Paralelismo local do Spark

Todas as execuções: janelas de 1 minuto, `CLASSIFIER_MODE=embedding`, taxonomia de 12 categorias.

| `SPARK_MASTER` | CPU (%) | RAM (MB) | Execuções |
|---|---|---|---|
| `local[1]` | 103,6 | 1.519 | 1 |
| `local[2]` | 215,7 | 1.993 | 1 |
| `local[4]` | 211,2 | 2.024 | 1 |

**Discussão**: a CPU escala linearmente de `local[1]` para `local[2]` (~104% → ~216%), mostrando que o pipeline usa dois núcleos completos efetivamente quando disponíveis. Porém, ir de `local[2]` para `local[4]` não produz nenhum ganho mensurável (215,7% → 211,2%). Esta é a confirmação mais clara de que o sistema é **limitado pela entrada a ~29 ev/s**: o stream da Wikimedia não consegue alimentar 4 threads com rapidez suficiente. Dois threads é o ponto de saturação para esta taxa de entrada; além disso, threads adicionais somam overhead de escalonamento JVM sem benefício de throughput.

---

#### Experimento 3 — Tamanho da janela de agregação

Todas as execuções: `CLASSIFIER_MODE=embedding`, `SPARK_MASTER=local[*]`, taxonomia de 12 categorias.

| Duração da janela | CPU (%) | RAM (MB) | Execuções |
|---|---|---|---|
| 1 min | 206,7 | 1.961 | 1 |
| 5 min | 212,9 | 2.020 | 1 |
| 10 min | 210,8 | 2.628 | 1 |

**Discussão**: a CPU é praticamente constante em todos os tamanhos de janela (~207–213%), confirmando que a etapa de classificação (WORKLOAD-1) domina o custo de CPU e roda na mesma taxa independentemente de quanto estado o Spark acumula. O tamanho da janela só afeta quantos eventos são mantidos no estado de agregação antes de um flush. A RAM conta a história real: a janela de 10 minutos requer 34% mais memória que a de 1 minuto (+667 MB), causado pelo maior estado de watermark que o Spark precisa manter para tratar chegadas tardias. Para uso em produção, a janela de 5 minutos oferece o melhor equilíbrio: overhead de RAM insignificante em relação à baseline de 1 minuto (+59 MB) com granularidade útil para detecção de tendências.

---

#### Snapshot de dados reais — janela de produção de 5 minutos

Capturado durante os testes funcionais, janela 00:05–00:10 UTC, janela tumbling de 5 minutos.

| Categoria | Eventos | Participação (%) | Crescimento vs. janela anterior |
|---|---|---|---|
| Politics | 2.195 | 24,8 | ×5,4 |
| History | 1.233 | 14,0 | ×4,6 |
| Health | 1.130 | 12,8 | ×5,0 |
| Geography | 1.041 | 11,8 | ×4,9 |
| Arts | 961 | 10,9 | ×6,1 |
| Technology | 657 | 7,4 | ×3,6 |
| Sports | 354 | 4,0 | ×6,9 |
| Science | 351 | 4,0 | ×2,8 |
| Entertainment | 317 | 3,6 | ×10,2 |
| Business | 235 | 2,7 | ×5,2 |
| Military | 195 | 2,2 | ×16,3 |
| Religion | 166 | 1,9 | ×3,3 |
| **Total** | **8.835** | 100 | ×5,0 |

Politics é consistentemente a categoria mais editada. Military mostrou o maior crescimento entre janelas (×16,3), consistente com picos de cobertura de eventos ao vivo no período de coleta.

> **Nota sobre métricas**: "Participação" (`count / max_count × 100`) mede o volume relativo dentro de uma única janela — identifica a categoria dominante, não a que mais cresceu. O crescimento entre janelas é capturado pela coluna "Crescimento" acima. As duas métricas respondem perguntas diferentes e não devem ser confundidas.

---

## 7. Limitações e conclusões

### O que funcionou

O pipeline está totalmente funcional de ponta a ponta. Todos os 7 testes funcionais e 2 de integração passaram sem falhas. O classificador atingiu 86,7% de acurácia com `all-MiniLM-L6-v2`, acima da meta de 80%. A recuperação após reinício de um container leva 8 segundos sem perda de dados, graças ao checkpointing do Spark e à retenção de offsets do Kafka.

Todos os 4 experimentos de benchmark produziram tendências claras e consistentes:

- **Embedding como cadenciador (Exp 1)**: o classificador desacelera o Spark para uma taxa sustentável, paradoxalmente reduzindo à metade o uso de CPU em comparação com rodar sem modelo. O sistema é limitado pela entrada (~29 ev/s do stream da Wikimedia), não pelo processamento.
- **Mais categorias = menos CPU (Exp 4)**: o custo de agregação domina, não o custo de similaridade coseno. Distribuir eventos por mais grupos reduz o trabalho por partição no Spark.
- **Teto de paralelismo em 2 threads (Exp 2)**: local[2] e local[4] consomem o mesmo CPU (~213%), confirmando a hipótese de limitação pela entrada — mais threads não ajudam se a fonte não consegue alimentá-las mais rápido.
- **Tamanho da janela afeta RAM, não CPU (Exp 3)**: CPU é constante (~208%) em janelas de 1/5/10 minutos; RAM cresce +34% em 10 minutos devido ao estado de watermark. O padrão de 5 minutos é o trade-off ótimo.

### Desafios de implementação

- **PyTorch dentro de um Pandas UDF do Spark**: o modelo sentence-transformer não pode ser serializado pela fronteira JVM↔Python da forma usual. O modelo deve ser inicializado de forma lazy *dentro* da UDF na primeira chamada. Acertar isso, e evitar múltiplos carregamentos redundantes por executor, foi a parte mais difícil da integração.
- **Conteúdo multilíngue**: eventos da Wikimedia chegam em 300+ idiomas. O modelo `all-MiniLM-L6-v2` (somente inglês) classifica incorretamente ou ignora títulos não ingleses. O modelo pretendido para produção é `intfloat/multilingual-e5-base`, mas é muito lento para hardware somente CPU. Em GPU, é a escolha correta.
- **Q-IDs do Wikidata**: artigos no namespace Wikidata têm títulos como `Q123456`, que não carregam conteúdo semântico. O pipeline detecta esses títulos com uma regex e substitui o campo `parsedcomment` (o resumo da edição em linguagem natural) como texto de classificação.
- **Configuração de listeners do Kafka**: tornar o Kafka acessível tanto de dentro do Docker (inter-container) quanto do host exigiu nomes de listener separados (`PLAINTEXT` vs `PLAINTEXT_HOST`) com endereços anunciados diferentes. Configurar incorretamente produzia falhas silenciosas onde o produtor aparentava enviar, mas o Spark nunca recebia.
- **Acesso concorrente ao SQLite**: o Streamlit fazendo polling no SQLite enquanto o Spark estava escrevendo causava erros `database is locked` com o modo de journal padrão. Mudar para modo WAL resolveu — WAL permite múltiplos leitores simultâneos com um único escritor sem risco de corrupção.
- **Tempo de evento vs. tempo de processamento**: usar timestamps de evento (quando a edição aconteceu) em vez do tempo de ingestão para o `window()` exigiu ajuste cuidadoso do watermark. Um watermark muito curto descarta eventos atrasados; muito longo mantém estado desnecessário em memória.

### Decisões de design

| Decisão | Alternativa considerada | Por que escolhemos assim |
|---|---|---|
| **Kafka como buffer** | Consumidor SSE direto no Spark | Kafka retém mensagens se o Spark reiniciar; desacopla a taxa do produtor da velocidade do consumidor |
| **SQLite como sink** | PostgreSQL, Redis | Arquivo único, sem container extra, modo WAL suporta nossa taxa de escrita; suficiente para um dashboard com um único leitor |
| **Spark em modo local** | Cluster distribuído YARN/K8s | O modelo sentence-transformer deve estar disponível em cada nó executor; o modo local evita distribuir um binário de modelo de 90 MB |
| **Sentence-transformers** | Regras de palavras-chave, API LLM | Regras de palavras-chave falham para títulos não ingleses (60%+ das edições da Wikimedia); APIs LLM adicionam latência e custo; embeddings funcionam de forma multilíngue na velocidade de inferência |
| **Pandas UDF para classificação** | Python UDF linha por linha | Uma chamada `model.encode()` em batch por micro-batch vs. uma chamada por linha — ordens de magnitude mais rápido devido à vetorização GPU/CPU |

### Limitações

- **Spark em nó único**: o pipeline roda em modo `local[*]` dentro de um único container. Modo distribuído (YARN / Kubernetes) exigiria implantar o modelo sentence-transformer em todos os nós worker, o que não é trivial para modelos grandes.
- **SQLite como sink**: o SQLite lida bem com nossa taxa de escrita em micro-batches de 1 segundo, mas se tornaria um gargalo sob leituras paralelas intensas (múltiplas instâncias de dashboard) ou se a duração do micro-batch cair abaixo de ~200 ms.
- **Taxonomia estática**: as categorias são fixadas na inicialização. Adicionar ou remover uma categoria exige reiniciar o `spark_consumer` para que os embeddings de categoria sejam recomputados.
- **Qualidade da classificação**: similaridade coseno contra uma única string de rótulo é um proxy forte, mas imperfeito. Títulos com ambiguidade semântica genuína (ex: "Mercury" — Science, Geography ou Entertainment?) sempre serão debatíveis. A acurácia de 86,7% em uma amostra de 30 títulos é indicativa, não estatisticamente conclusiva.
- **Throughput limitado pela entrada**: a taxa de ~29 ev/s é determinada pelo stream da Wikimedia, não pela capacidade do sistema. Benchmarks com uma fonte sintética controlada (ex: um produtor de replay em taxa variável) dariam um quadro mais limpo do teto real de processamento.
- **Ambiente somente CPU**: o modelo multilíngue (`intfloat/multilingual-e5-base`) não foi avaliado em benchmark. Em hardware com GPU, seria o modelo preferido para produção; em CPU é muito lento para experimentação iterativa.
- **Uma execução por configuração**: cada configuração de benchmark foi executada uma vez devido ao custo de tempo (~5 minutos por configuração × 11 configurações ≈ 1,5 hora no total). Desvio padrão não pode ser reportado; os resultados são direcionais.

---

## 8. Referências e recursos externos

- [Documentação do Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams)
- [Documentação do Apache Kafka 3.7](https://kafka.apache.org/37/documentation.html)
- [Guia do Apache Spark 3.5 Structured Streaming](https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html)
- [Biblioteca sentence-transformers](https://www.sbert.net/)
- Modelo: [intfloat/multilingual-e5-base](https://huggingface.co/intfloat/multilingual-e5-base) — Wang et al., 2024
- Modelo: [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) — Reimers & Gurevych, 2019
- [Documentação do Streamlit](https://docs.streamlit.io/)
- [PyTorch 2.7 + CUDA 12.8 wheels](https://download.pytorch.org/whl/cu128)
