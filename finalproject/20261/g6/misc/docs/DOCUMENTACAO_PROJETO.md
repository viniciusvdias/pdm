# Detecção Distribuída de Comunidades com Label Propagation

## Documentação do Projeto — Código, Arquitetura e Uso

**Repositório:** [FelipeOliveira456/big-data](https://github.com/FelipeOliveira456/big-data)  
**Pacote Python:** `distributed-lpa`  
**Dataset:** soc-Orkut (SNAP, grafo **não direcionado**; simetrizado na carga)  
**Abordagens:** Ray vs Dask (LPA síncrono distribuído)

---

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Objetivos](#2-objetivos)
3. [O algoritmo LPA](#3-o-algoritmo-lpa)
4. [Dataset soc-Orkut](#4-dataset-soc-orkut)
5. [Arquitetura do software](#5-arquitetura-do-software)
6. [Estrutura de diretórios](#6-estrutura-de-diretórios)
7. [Módulos e responsabilidades](#7-módulos-e-responsabilidades)
8. [Carga do grafo](#8-carga-do-grafo)
9. [Implementação Ray](#9-implementação-ray)
10. [Implementação Dask](#10-implementação-dask)
11. [Benchmark e relatórios](#11-benchmark-e-relatórios)
12. [Interface de linha de comando](#12-interface-de-linha-de-comando)
13. [Docker (1 VM)](#13-docker-1-vm)
14. [Testes e QA](#14-testes-e-qa)
15. [Requisitos de hardware](#15-requisitos-de-hardware)
16. [Decisões de engenharia](#16-decisões-de-engenharia)
17. [Referências](#17-referências)

---

## 1. Visão geral

Este projeto implementa e compara **duas variantes distribuídas** de **Label Propagation (LPA)** para detecção de comunidades:

| Abordagem | Tecnologia | Descrição |
|-----------|------------|-----------|
| **Ray** | Python + Ray Core | LPA com `@ray.remote` e chunks de índices |
| **Dask** | Python + Dask Distributed | LPA com `Client.submit` e chunks de índices |

Ambas leem o **mesmo grafo** — carregado do ficheiro SNAP ou de um fixture `.npz` — garantindo comparação justa de modularidade e número de comunidades.

A lógica central (iteração LPA, modularidade Q) fica em `lpa_core/` e `graph/`. Ray e Dask são camadas de paralelização do loop síncrono na **mesma VM**.

---

## 2. Objetivos

1. Detectar comunidades na rede **soc-Orkut** (~3,07M nós, ~117M arestas não direcionadas → ~234M arcos CSR).
2. Comparar **desempenho** (tempo, memória RSS, throughput) e **qualidade** (modularidade Q, número de comunidades).
3. Executar numa **única VM** (local ou Docker); integração E2E usa fixture **0,1%** (~1,6k nós).
4. Fornecer **testes unitários**, integração E2E e pipeline **Docker**.

---

## 3. O algoritmo LPA

Referência: Raghavan, Albert & Kumara (2007).

### Regra de atualização

- Cada nó começa com um rótulo (permutação dos ids dos nós, reproduzível por `seed`).
- Em cada iteração, cada nó adota o rótulo **mais frequente entre os vizinhos de saída** (peso da aresta soma votos).
- Em empate, escolhe o **menor rótulo** (determinístico).
- **Snapshot síncrono:** todos leem os rótulos do início da iteração e aplicam atualizações ao fim.

### Critério de parada

- Convergência quando **nenhum nó muda de rótulo** numa iteração, ou
- `max_iter` atingido (padrão **100**).

Não há número inicial de clusters *K* — o LPA descobre comunidades pelo consenso local.

<p align="center">
  <img src="assets/lpa-propagation-iterations.png" alt="Label Propagation: rótulos propagam por iteração" width="640"/>
  <br/>
  <sub>Neo4j — <a href="https://neo4j.com/blog/graph-data-science/graph-algorithms-neo4j-label-propagation/">Label Propagation</a> (Raghavan et al., 2007).</sub>
</p>

### Modularidade Q

Após as iterações, calcula-se **Q** (Blondel et al., 2008) sobre a partição final:

**Q = (1/2m) × Σ_ij ( A_ij − (k_i × k_j)/(2m) ) × δ(c_i, c_j)**

Implementação em `graph/modularity.py`.

---

## 4. Dataset soc-Orkut

| Atributo | Valor |
|----------|-------|
| Fonte | [SNAP — com-Orkut](https://snap.stanford.edu/data/com-Orkut.html) |
| Arquivo local | `data/raw/soc-orkut-relationships.txt` (baixado de `com-orkut.ungraph.txt.gz`) |
| Nós (~100%) | ~3.072.441 |
| Arestas não direcionadas (SNAP) | ~117,2M |
| Arcos CSR (simetrizados) | ~234M |
| Tamanho raw | ~4,6 GB (descomprimido) |

### Tratamento na carga

1. Ignorar cabeçalho e linhas inválidas.
2. Grafo **não direcionado**: cada linha `u v` gera arcos `u→v` e `v→u` (simetrização em numpy, uma passagem).
3. Remover self-loops e duplicatas `(src, dst)`.
4. Grafos grandes: amostra **BFS conectada** por fração (seed **42**); BFS trata arestas como não direcionadas.
5. Subgrafo **induzido**; construção **out-CSR** em memória (numpy).

Não há pipeline Parquet nem artefatos intermediários em produção — o benchmark lê o TXT diretamente.

### Fixture de integração (0,1%)

| Atributo | Valor |
|----------|-------|
| Ficheiro | `tests/integration/fixtures/orkut_0p1pct.npz` |
| Nós | 1.632 |
| Arcos CSR | ~9.774 |
| Metadados | `orkut_0p1pct.meta.json` |

Gerar com:

```bash
bash scripts/download_dataset.sh   # opcional para fixture sintético
bash scripts/build_integration_fixture.sh
```

---

## 5. Arquitetura do software

```text
soc-orkut-relationships.txt (SNAP)
       |
       v
preprocessing/load_graph.py  -->  out-CSR em memória (Graph)
       |
   +---+---+---+
   |       |   |
   v       v   v
lpa_ray  lpa_core  lpa_dask
 (Ray)   + graph    (Dask)
   |       |   |
   +---+---+---+
       v
benchmark/  -->  metrics_raw_<timestamp>.csv
              -->  partitions_<timestamp>/
              -->  comparison_<timestamp>.md
```

**Princípios:**

- Ray e Dask **não importam um ao outro**.
- Núcleo LPA e grafo **out-CSR** em `lpa_core/` + `graph/`.
- Workers e chunks: **auto = número de CPUs** (`LPA_WORKERS` para fixar).
- Config via `config.yaml` + variáveis de ambiente.

---

## 6. Estrutura de diretórios

```text
big-data/
├── src/
│   ├── lpa_core/           # LPA síncrono (núcleo puro)
│   ├── graph/              # Grafo out-CSR + modularidade Q
│   ├── preprocessing/      # SNAP → CSR em memória; fixtures .npz
│   ├── ray_impl/           # LPA Ray
│   ├── dask_impl/          # LPA Dask
│   ├── benchmark/          # Métricas, CSV, relatório MD
│   ├── cli/                # Entrypoint CLI
│   └── config.py           # Config YAML + env vars
├── tests/
│   ├── unit/               # pytest (85 testes)
│   └── integration/        # E2E Orkut 0,1% (fixture .npz)
├── data/
│   └── raw/                # soc-orkut-relationships.txt (gitignored)
├── reports/                # Saídas locais timestampadas (gitignored)
├── results/                # Relatório analítico + figuras (campanha arquivada)
├── scripts/                # download, docker, QA, fixture
├── docs/                   # Esta documentação
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── config.yaml.example
└── README.md
```

---

## 7. Módulos e responsabilidades

### `graph/`

| Ficheiro | Função |
|----------|--------|
| `graph.py` | Grafo **out-CSR** (numpy): `node_ids`, `indptr`, `neighbors`; `from_edges`, `from_coo`, `from_csr_arrays` |
| `modularity.py` | Modularidade Q sobre partição final |

### `lpa_core/`

| Ficheiro | Função |
|----------|--------|
| `lpa.py` | Iteração LPA, chunks, `run_lpa_sequential`, `LpaResult` |
| `distributed.py` | Loop síncrono compartilhado Ray/Dask |
| `worker_memory.py` | Pico RSS por worker (hostname) |

### `preprocessing/`

| Ficheiro | Função |
|----------|--------|
| `load_snap.py` | Leitura streaming do SNAP; COO direcionado |
| `load_graph.py` | Orquestra carga TXT → out-CSR (pequeno em memória; grande com passagem streaming) |
| `load.py` | Dispatch: `.npz` (fixture) ou SNAP |
| `sample_lcc.py` | Amostragem BFS conectada, subgrafo induzido, LCC |
| `graph_artifact.py` | Save/load fixtures `.npz` + `.meta.json` (integração) |
| `fractions.py` | Helpers de frações percentuais |

### `ray_impl/lpa_ray.py`

- `@ray.remote lpa_chunk_remote`: uma iteração num chunk de índices.
- Modo **local:** `ray.init(num_cpus=...)` na mesma VM.
- Modo **cluster** (opcional): `ray.init(address="ray://<head>:10001")`.
- Workers alinhados ao número de chunks (`lpa_chunk_divisor`).

### `dask_impl/lpa_dask.py`

- `client.submit(lpa_iteration_chunk_tracked, ...)` por chunk.
- Modo **local:** `LocalCluster(n_workers=...)` na mesma VM.
- Modo **cluster** (opcional): `Client("tcp://<scheduler>:8786")`.

### `benchmark/`

| Ficheiro | Função |
|----------|--------|
| `runner.py` | Campanha N runs × 2 abordagens × frações → CSV |
| `metrics.py` | tracemalloc + **psutil RSS** (driver + árvore de processos) |
| `partitions.py` | `summary.json` + `communities.json` (sem listas de nós > 50k) |
| `memory_estimate.py` | Extrapolação tempo/RAM para 100% (calibração 0,1% e 10%) |
| `seeds.py` | Seeds por run (`seed`, `seed+1`, … ou `BENCHMARK_SEEDS`) |
| `vm_memory.py` | Pico RSS por hostname |
| `paths.py` | Timestamps `YYYYMMDDTHHMMSS` |
| `report.py` | Markdown comparativo (média ± desvio) |

### `cli/main.py`

Subcomandos: `lpa-ray`, `lpa-dask`, `benchmark`, `report`.

---

## 8. Carga do grafo

### Fluxo (SNAP TXT)

1. Coletar nós da LCC global do ficheiro SNAP.
2. Por fração solicitada (0.1 / 1 / 10 / 100):
   - Amostrar nós conectados com `random.Random(seed)` (BFS).
   - Filtrar arestas direcionadas (ambos endpoints no conjunto).
   - Construir **out-CSR** em memória.
3. Grafos grandes (~850 MB): passagem streaming; sem materializar todas as arestas antes da amostra.

### Fixture `.npz` (só testes)

Arrays CSR comprimidos + metadados JSON — carga em milissegundos para E2E.

### Uso na CLI

O parâmetro `--input` aceita o TXT SNAP ou um `.npz`. A fração (`--fraction` / `--fractions`) aplica-se apenas ao TXT.

---

## 9. Implementação Ray

### Paralelização — chunks por índice

1. Particionar índices de nós em chunks ≈ `lpa_chunk_divisor` (padrão: **número de CPUs**).
2. Cada iteração: `ray.put` do snapshot de rótulos; workers processam chunks em paralelo.
3. Driver aplica atualizações e verifica convergência.
4. Loop compartilhado em `lpa_core/distributed.py`.

### Comando

```bash
python -m cli.main lpa-ray \
  --input data/raw/soc-orkut-relationships.txt \
  --fraction 100
```

Dashboard (modo local): http://localhost:8265

---

## 10. Implementação Dask

Mesma lógica de chunks e snapshot síncrono que Ray, via `Client.submit`.

### LocalCluster (padrão)

| Config | Comportamento |
|--------|---------------|
| `dask_scheduler_address: null` | Cria `LocalCluster` na máquina |
| `dask_n_workers: null` | Auto (= `lpa_chunk_divisor` / CPUs) |
| `dask_scheduler_address: host:8786` | Conecta ao scheduler remoto (opcional) |

### Comando

```bash
python -m cli.main lpa-dask \
  --input data/raw/soc-orkut-relationships.txt \
  --fraction 100
```

Dashboard (LocalCluster): http://localhost:8787

---

## 11. Benchmark e relatórios

### CSV (`reports/metrics_raw_YYYYMMDDTHHMMSS.csv`)

Colunas principais:

| Coluna | Descrição |
|--------|-----------|
| `approach` | `ray` ou `dask` |
| `fraction_pct` | 0.1, 1, 10, 100, … |
| `seed` | Semente da run (42, 43, 44 em 3 runs) |
| `graph_load_time_s` | Tempo TXT → CSR |
| `init_time_s` | Tempo de `ray.init` / criação do cluster |
| `algorithm_time_s` | Tempo do algoritmo |
| `peak_driver_rss_mb` | RSS do processo driver |
| `peak_process_tree_rss_mb` | RSS driver + workers filhos |
| `peak_cluster_rss_mb` | Soma dos picos por hostname |
| `vm_peaks_json` | Pico RSS por hostname |
| `modularity_q` | Modularidade final |
| `num_communities` | Comunidades na partição final |
| `level_times_json` | Tempos por iteração |
| `communities_json` | Caminho do JSON legível de clusters |

### Partições (`reports/partitions_<stamp>/`)

- `*.summary.json` — métricas + caminhos
- `*.communities.json` — clusters ordenados por tamanho (sem `node_ids` se n > 50k)

### Markdown (`reports/comparison_YYYYMMDDTHHMMSS.md`)

```bash
python -m cli.main benchmark --input data/raw/soc-orkut-relationships.txt --fractions 100 --runs 3
python -m cli.main report
```

Com `--append`, reutiliza o stamp da run anterior (útil no Docker quando Ray e Dask rodam em sequência).

Relatório analítico completo (figuras + análise de desempenho): [`results/REPORT.md`](../results/REPORT.md).

### Resultados experimentais — soc-Orkut 100%

Campanha de referência **`20260622T005654`** (VM Docker, 6 vCPUs, ~16 GB RAM, 3 072 441 nós, 100 iter, seeds 42/43/44). Dados arquivados em `results/reports/`; gráficos em `results/figures/`.

#### Resumo executivo

| Métrica | Ray (3/3) | Dask (3/3*) | Razão Ray/Dask |
|---------|-----------|-------------|----------------|
| Tempo médio do algoritmo | **648,8 s** ± 13,3 | 1298 s ± 36 | **~2,0×** |
| Throughput | **4704 nós/s** ± 96 | 2368 nós/s ± 67 | **~2,0×** |
| RSS pico (`peak_process_tree_rss_mb`) | **10,9 GB** ± 0,1 | 11,7 GB ± 0,1 | ~7% menos RAM |
| Comunidades finais | 590 | 590 | **idênticas** |

\* **Dask run 1 (seed 42):** sucesso na campanha isolada `20260622T030138` (1333 s). Na campanha mista `005654`, a run 1 **falhou por OOM** — **2 falhas** no total (também `024351`). O gráfico usa a run isolada para run 1 (barra hachurada).

As partições finais são **equivalentes** entre todas as runs bem-sucedidas — inicialização LPA determinística por `node_id`.

#### Desempenho por run

| Run | Seed | Abordagem | Algoritmo (s) | Total (s) | Throughput | RSS pico |
|-----|------|-----------|---------------|-----------|------------|----------|
| 1 | 42 | Ray | 667,4 | 1035,6 | 4604 n/s | 11,1 GB |
| 2 | 43 | Ray | 637,4 | 1004,3 | 4820 n/s | 10,8 GB |
| 3 | 44 | Ray | 641,6 | 1008,2 | 4789 n/s | 10,8 GB |
| 1 | 42 | Dask | 1333,4* | 1682,5* | 2304 n/s | 11,9 GB |
| 2 | 43 | Dask | 1313,1 | 1682,0 | 2340 n/s | 11,9 GB |
| 3 | 44 | Dask | 1248,6 | 1617,8 | 2461 n/s | 12,0 GB |

\* Run isolada (`030138`), não a tentativa OOM da campanha mista.

**Custo por iteração:** Ray ~6,1–6,8 s/iter; Dask ~12,5–13,3 s/iter (média), com picos até ~23 s.

![Comparação Ray vs Dask](../results/figures/performance_comparison.png)

![Tempo por iteração](../results/figures/iteration_times.png)

#### Qualidade da clusterização

| Estatística | Valor |
|-------------|-------|
| Comunidades | 590 |
| Maior comunidade | 1 437 404 nós (46,8%) |
| 2.ª maior | 1 372 668 nós (44,7%) |
| Top 10 | 98,7% dos nós |
| `converged` | false (100 iter) |

Partição **fortemente desbalanceada** — típico de LPA em rede social densa. Com >3M nós, os JSON **não incluem `node_ids`** (limite 50k).

Gera `performance_comparison.png`, `iteration_times.png` e `convergence_changed_nodes.png`.

Dask run 1 usa métricas da campanha isolada `20260622T030138` (sucesso); a nota no gráfico indica as 2 falhas OOM na campanha mista.

#### Lições operacionais

| Problema | Mitigação |
|----------|-----------|
| Ray `/dev/shm` 64 MB | `shm_size: 4gb` no Docker |
| Dask OOM após Ray | Reiniciar container; `LPA_WORKERS=4` |
| `converged=false` | Normal em Orkut com 100 iter |

---

## 12. Interface de linha de comando

| Comando | Descrição |
|---------|-----------|
| `lpa-ray` | LPA Ray num grafo (TXT ou `.npz`) |
| `lpa-dask` | LPA Dask num grafo |
| `benchmark` | Campanha completa (N runs × abordagens) |
| `report` | Gera Markdown a partir do CSV |

### Configuração (`config.yaml` / env vars)

| Chave / env | Padrão | Descrição |
|-------------|--------|-----------|
| `graph_raw_path` / `GRAPH_RAW_PATH` | `data/raw/soc-orkut-relationships.txt` | Ficheiro SNAP |
| `dataset_slug` | `orkut` | Prefixo nos nomes de partição |
| `seed` / `SEED` | `42` | Semente base (amostra + LPA) |
| `lpa_max_iter` / `LPA_MAX_ITER` | `100` | Máximo de iterações |
| `graph_directed` / `GRAPH_DIRECTED` | `false` | `true` = SNAP direcionado (sem simetrizar) |
| `lpa_chunk_divisor` / `LPA_CHUNK_DIVISOR` | auto (CPUs) | Número de chunks por iteração |
| `LPA_WORKERS` | — | Fixa workers e chunk divisor |
| `ray_num_cpus` / `RAY_NUM_CPUS` | auto | CPUs Ray (local) |
| `dask_n_workers` / `DASK_N_WORKERS` | auto | Workers Dask (local) |
| `ray_head_address` / `RAY_HEAD_ADDRESS` | — | Head Ray remoto (opcional) |
| `dask_scheduler_address` / `DASK_SCHEDULER_ADDRESS` | — | Scheduler Dask remoto (opcional) |
| `BENCHMARK_RUNS` | `3` | Repetições por abordagem/fração |
| `BENCHMARK_SEEDS` | `42,43,44` | Seeds explícitas (opcional) |
| `BENCHMARK_STAMP` | auto | Stamp fixo para Ray+Dask no Docker |

Pipeline completo no host:

```bash
bash scripts/run_all.sh   # requer venv em .venv
```

---

## 13. Docker (1 VM)

Grafo e workers ficam na **mesma máquina**. Ray e Dask rodam em fases separadas (workers locais, 1 processo por CPU).

### Build e execução

```bash
docker build -t distributed-lpa:latest .
bash scripts/run-docker.sh
# ou:
docker compose up --build
```

Volumes: `./data`, `./reports`. Config montada de `config.yaml.example`.

### Variáveis (`docker-compose.yml`)

| Variável | Default | Descrição |
|----------|---------|-----------|
| `GRAPH_RAW_PATH` | `data/raw/soc-orkut-relationships.txt` | Grafo SNAP |
| `BENCHMARK_FRACTIONS` | `100` | Fração percentual (100 = grafo completo LCC) |
| `BENCHMARK_RUNS` | `3` | Repetições |
| `BENCHMARK_BACKEND` | `both` | `ray` → `dask` → `report` |
| `LPA_WORKERS` | auto | CPUs do container |

O entrypoint (`scripts/docker-entrypoint.sh`) gera um `BENCHMARK_STAMP` único, corre Ray, depois Dask com `--append`, e gera o relatório.

---

## 14. Testes e QA

### Setup

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Testes

```bash
pytest tests/unit/ -v
pytest tests/integration/ -m integration -v -s
pytest tests/integration/test_lpa_orkut.py -m integration -v -s --backend ray
```

E2E grava em `tests/integration/output/` (não toca em `reports/`).

Fixture: `bash scripts/build_integration_fixture.sh` (raw Orkut ou sintético).

### QA

```bash
make qa-check    # ruff + pylint + bandit
make qa          # + pytest+coverage + mutmut
# ou:
bash scripts/run_qa.sh
```

---

## 15. Requisitos de hardware

### Grafo em memória (out-CSR numpy)

| Fração | Arestas direcionadas (~) | Pico driver (ordem de grandeza) |
|--------|--------------------------|----------------------------------|
| 0,1% | ~4k | dezenas de MB |
| 1% | ~40k | ~100 MB |
| 10% | ~1,9M | ~1 GB |
| 100% | ~22M | ~2–4 GB (TXT → CSR) |

Carga grande usa passagem streaming sobre o TXT — evita duplicar o grafo inteiro em COO antes da amostra.

### VM recomendada (100% Orkut)

- **16+ GB RAM** para benchmark completo Ray + Dask em sequência (CSR ~234M arcos).
- CPUs: quanto mais, mais chunks paralelos (auto-detectado).

Estimativas detalhadas: `benchmark/memory_estimate.py` (calibrado com runs 0,1% e 10%).

---

## 16. Decisões de engenharia

| Decisão | Motivo |
|---------|--------|
| soc-Orkut não direcionado | Grafo maior; simetrização na carga (numpy, uma passagem) |
| out-CSR | Vizinhança O(grau) para propagação de rótulos |
| Carga TXT → CSR em memória | Menos I/O e disco que pipeline Parquet intermediário |
| LPA síncrono + snapshot | Mesmo modelo mental Ray/Dask; comparável e testável |
| Chunks por índice (não BFS) | Topologia irrelevante para LPA |
| `lpa_core` + `distributed.py` | Evitar duplicar loop Ray/Dask |
| Workers = CPU count | Aproveitar a VM única sem config manual |
| Fixtures `.npz` só em testes | E2E rápido sem re-ler 850 MB de TXT |
| Partições JSON (sem Parquet) | Saída legível; `node_ids` omitidos se n > 50k |
| RSS por hostname | Medir pico driver + árvore de processos |
| Relatórios timestampados | Evitar sobrescrever runs anteriores |
| Ray → Dask sequencial no Docker | Uma VM não aguenta dois clusters em paralelo |

---

## 17. Referências

1. Raghavan, U. N., Albert, R., & Kumara, S. (2007). Near linear time algorithm to detect community structures in large-scale networks. *Physical Review E*, 76(3), 036106.
2. Blondel, V. D. et al. (2008). Fast unfolding of communities in large networks. *J. Stat. Mech.* P10008.
3. Leskovec, J., Krevl, A. (2014). SNAP Datasets. https://snap.stanford.edu/data
4. Moritz, P. et al. (2018). Ray: A Distributed Framework for Emerging AI Applications. OSDI.
5. Dask documentation: https://docs.dask.org/

---

*Documentação alinhada ao código — branch `003-label-propagation-distributed`, Junho 2026.*
