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

| Component | Specification |
|---|---|
| GPU | NVIDIA GPU with CUDA 12.8 support (optional) |
| CUDA | 12.8 |
| CPU fallback | ≥ 32 GB RAM |
| Docker | latest stable |
| OS | Linux (host) |
| Spark mode | `local[*]` (single container) |
| Kafka partitions | 3 |
| Default window | 5 minutes |
| Default model | `intfloat/multilingual-e5-base` |

### 6.2 How to perform benchmarking

Each experiment is run by modifying variables in `.env`, rebuilding the `spark_consumer` image, and letting the pipeline run for at least 3 complete windows before collecting metrics.

Metrics are collected from:
- **Throughput**: events processed / second, read from Spark logs (`[SQLite]` upsert counts)
- **Latency**: average time per micro-batch, from Spark UI or logs
- **CPU / Memory**: `docker stats spark_consumer` during steady-state processing

Each configuration is run **at least 5 times**; average and standard deviation are reported.

```bash
# Example: 5-run benchmark loop
for i in {1..5}; do
  echo "=== Run $i ===" && docker compose up --build -d
  sleep 600   # let 2 full windows complete
  docker logs spark_consumer 2>&1 | grep -E "(micro-batch|SQLite|events)" >> results_run${i}.txt
  docker compose down -v
done
```

### 6.3 What did you test?

#### Experiment 1 — Impact of semantic embeddings (WORKLOAD-1)

Compare the full embedding pipeline against a lightweight keyword-matching baseline (no model, uses `title.rlike()` rules).

| Variable | Values |
|---|---|
| Pipeline variant | keyword baseline vs. embedding (MiniLM) vs. embedding (multilingual-e5) |
| Metrics | throughput (events/s), avg batch latency (ms), CPU %, memory (MB) |

#### Experiment 2 — Spark local parallelism

Vary the number of local Spark threads (`local[N]`). This is **not** a distributed cluster experiment — all threads run inside the same container. The goal is to measure how intra-process parallelism affects throughput and micro-batch latency on a single machine.

| Variable | Values |
|---|---|
| `spark.master` | `local[1]`, `local[2]`, `local[4]` |
| Metrics | throughput, avg micro-batch duration |

#### Experiment 3 — Aggregation window size (WORKLOAD-3)

Larger windows accumulate more state; smaller windows produce results faster.

| Variable | Values |
|---|---|
| `WINDOW_DURATION` | 1 min, 5 min, 10 min |
| Metrics | Spark state memory, time to first result, result stability |

#### Experiment 4 — Number of categories (WORKLOAD-1)

The cosine similarity matrix grows with the number of categories. Test the classification cost.

| Variable | Values |
|---|---|
| Category count | 5, 10, 12 (full taxonomy) |
| Metrics | classification throughput, avg similarity computation time |

### 6.4 Results

> **To be filled after running the experiments.**

Results tables will follow the format below (example columns; actual measurements pending):

#### Experiment 1 — Embedding impact

| Pipeline variant | Avg throughput (events/s) | Std Dev | Avg latency (ms) | Std Dev | Runs |
|---|---|---|---|---|---|
| Keyword baseline | — | — | — | — | 5 |
| MiniLM-L6-v2 | — | — | — | — | 5 |
| multilingual-e5-base | — | — | — | — | 5 |

#### Experiment 2 — Spark executors

| Threads (`local[N]`) | Avg throughput (events/s) | Std Dev | Runs |
|---|---|---|---|
| 1 | — | — | 5 |
| 2 | — | — | 5 |
| 4 | — | — | 5 |

#### Experiment 3 — Window size

| Window duration | Time to first result (s) | Avg state memory (MB) | Std Dev | Runs |
|---|---|---|---|---|
| 1 min | — | — | — | 5 |
| 5 min | — | — | — | 5 |
| 10 min | — | — | — | 5 |

#### Experiment 4 — Category count

| Categories | Avg classification throughput (events/s) | Std Dev | Runs |
|---|---|---|---|
| 5 | — | — | 5 |
| 10 | — | — | 5 |
| 12 | — | — | 5 |

---

## 7. Limitations and conclusions

> **To be filled after running the experiments.**

Known design limitations:

- **Single-node Spark**: the pipeline runs in `local[*]` mode inside one container. Distributed mode (YARN / Kubernetes) would require exporting the sentence-transformer model to all worker nodes.
- **SQLite as sink**: SQLite handles our write rate well at 1-second micro-batches, but would become a bottleneck under heavy parallel reads (multiple dashboard instances) or if micro-batch duration drops below ~200 ms.
- **Taxonomy is static**: categories are fixed at startup. Adding or removing a category requires restarting the `spark_consumer` container so the category embeddings are re-computed.
- **Classification quality**: cosine similarity against a single description string is a strong but imperfect proxy. Articles with ambiguous titles (e.g. "Mercury" could be Science, Geography, or Entertainment) may be misclassified.

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
