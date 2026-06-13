# Final project report: *Motor de Liquidação estilo Pix com Apache Flink (PyFlink)*

> Grupo **G3** — PDM 2026/1. Tema: stream processing, event-time e exactly-once
> aplicados a um motor de liquidação financeira sobre o dataset **PaySim**.

## 1. Context and motivation

O **Pix** processa, no Brasil, dezenas de milhões de transações por dia com
liquidação em segundos. Construir o motor que faz essa liquidação impõe três
requisitos não negociáveis, que mapeiam diretamente aos três pilares do tema:

1. **Latência baixíssima** → *stream processing* (a confirmação tem que sair em segundos);
2. **Correção financeira absoluta** → *exactly-once* (nenhuma transação contabilizada
   duas vezes ou perdida — diferença de um centavo já é falha);
3. **Resiliência a falhas** → *event-time + checkpointing* (o sistema cai, sobe, e o
   saldo final tem que estar certo).

Este projeto implementa, **em Python (PyFlink)**, o serviço de liquidação ponta a
ponta com Apache Flink, incluindo **detecção de lavagem de dinheiro** (padrão
circular `A→B→C→A`, *structuring* e velocidade) como bônus. A demonstração
definitiva: subir o fluxo de transações, **matar um TaskManager no meio**, e
verificar que o saldo final de cada conta **bate ao centavo** com um baseline
batch determinístico calculado sobre o mesmo input.

### Por que é "big data"
- Volume real do Pix: 100M+ transações/dia em produção; o experimento simula uma
  instituição média e **amplifica** o PaySim para ≥1 GB (até ~5 GB).
- Estado por conta (saldo, histórico recente, contrapartes) cresce para milhões de
  contas — não cabe em RAM única; exige estado particionado + RocksDB.
- Reprocessamento determinístico para conciliação contábil é custoso e a janela de
  tolerância a falhas é apertada.

## 2. Data

### 2.1 Detailed description

**Fonte primária: PaySim** — simulador open-source de transações de *mobile money*
baseado em logs reais de uma operadora africana.

- Origem: Kaggle — <https://www.kaggle.com/datasets/ealaxi/paysim1>
- ~6,36 milhões de transações, ~470 MB (CSV).
- Schema: `step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest,
  oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud`.
- Tipos: `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER`. Apenas `TRANSFER`
  é conta→conta (`C...`→`C...`), o que viabiliza o padrão de lavagem circular.
- Rótulo `isFraud` usado para avaliar precision/recall do AML.

**Por que PaySim e não o IBM TabFormer** (também disponível, ~2,4 GB de transações
de cartão): o schema do PaySim casa **1:1** com o workload de liquidação (contas
origem/destino e saldos), oferece *ground-truth* para a reconciliação e é o único
que permite o padrão circular `A→B→C→A`. O TabFormer (cartão→merchant, sem
conta-destino) ficou como validação cruzada opcional, fora do escopo central.

**Normalização determinística** (`src/common/schema.py`):
- `amount` → **centavos inteiros** (reconciliação exata, sem erro de float);
- `event_time` é **função pura** do índice da linha + `transaction_id`
  (`BASE_EPOCH + idx·10ms + jitter(tid)∈[0,2s)`), gerando out-of-orderness
  **limitada** que exercita watermarks sem perder reprodutibilidade;
- `transaction_id` = hash determinístico (unicidade/idempotência).

**Modelo de liquidação** (`src/common/ledger.py`, idêntico em stream e batch):
saldo inicial constante por conta (`OPENING_BALANCE_CENTS`); cada transação
expande em ops keyed por conta (`CASH_IN`→crédito; `PAYMENT`/`DEBIT`→débito;
`TRANSFER`/`CASH_OUT`→débito origem + crédito destino); o **débito só liquida se
houver saldo suficiente** (`valor ≤ saldo` ⇒ `SETTLED`, senão `REJECTED`), crédito
sempre soma. Como a aceitação por saldo **depende da ordem**, stream e batch
aplicam as ops de cada conta na MESMA ordem determinística `(event_time,
transaction_id, kind)`: o batch ordena globalmente; o operador PyFlink bufferiza
por conta e aplica em ordem de event-time (timer de event-time guiado por
watermark + timer de processing-time para drenar a cauda). O consumer reconstrói
o saldo final mantendo o op de maior **`apply_seq`** (sequência de aplicação por
conta) — o último efetivamente aplicado ao estado —, o que é robusto a eventos
atrasados. Simplificações documentadas na §7.

### 2.2 How to obtain the data

- **Amostra (versionada):** `datasample/sample.csv.gz` (~352 KB, < 1 MB) já está no
  repositório e é suficiente para o quick start. Foi gerada com:
  ```bash
  ./bin/make_sample.sh /caminho/paysim.csv datasample/sample.csv.gz
  ```
- **Dataset completo:** baixe o PaySim do Kaggle (link acima) e gere a versão
  amplificada (≥1 GB) — **não** versionamos datasets completos:
  ```bash
  # ex.: amplifica ×3 (~1,4 GB)
  ./bin/amplify.sh /caminho/paysim.csv data/paysim_x3.csv 3
  ```

## 3. How to install and run

> Requisito estrito: **apenas Docker**. Nenhuma outra instalação é necessária —
> todos os componentes (Kafka, Flink, MinIO, Postgres, Prometheus, Grafana,
> producer, consumer) rodam em containers via Docker Compose.

### 3.1 Quick start (sample data)

```bash
cd finalproject/20261/g3
./bin/run.sh              # equivalente a: docker compose up --build -d
```

O script sobe a stack, **submete os jobs PyFlink** (settlement + AML) e imprime as
URLs:

| Serviço | URL |
|---|---|
| Flink Web UI | http://localhost:8081 |
| Grafana (dashboards) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| MinIO console | http://localhost:9001 |

Acompanhe a liquidação e valide a reconciliação:
```bash
docker compose logs -f producer consumer
./bin/reconcile.sh        # saldo final do stream × baseline batch -> diff ZERO
```

Demonstração de falha (exactly-once):
```bash
docker compose up -d --scale flink-taskmanager=3
./bin/kill_taskmanager.sh # mata um TM; após recovery, reconcile continua ZERO
```

### 3.2 How to run with the full dataset

```bash
./bin/amplify.sh /caminho/paysim.csv data/paysim_x3.csv 3
./bin/run_full.sh data/paysim_x3.csv     # ajusta INPUT_CSV e sobe a stack
```

Parâmetros (paralelismo, intervalo de checkpoint, garantia, bursts, limiares AML)
ficam em `.env` (veja `.env.example`).

### 3.3 Slides da apresentação

O PDF é gerado de forma reprodutível (conteúdo em `src/presentation/make_slides.py`):
```bash
./bin/make_slides.sh        # cria presentation/presentation.pdf
```

## 4. Project architecture

```
[PaySim CSV (sample/amplificado)]
        │  producer (event-time acelerado, transaction_id único, BURSTS aleatórios)
        ▼
[Kafka  tx.raw] ──────────────────────────────────────────────┐
        │                                                      │
        ▼ (PyFlink, watermark event-time)                      ▼ (PyFlink)
[settlement_job]                                        [aml_job]
  flat_map → ops keyed por conta                          CYCLE A→B→C→A (paralelismo 1)
  KeyedProcessFunction (buffer + event-time timer)        VELOCITY / STRUCTURING (keyed)
  saldo por conta (RocksDB) → SETTLED/REJECTED            └─→ [Kafka aml.alerts]
  checkpoints incrementais → [MinIO s3://pix]
        │ sink EXACTLY_ONCE | AT_LEAST_ONCE
        ▼
[Kafka  tx.outcomes] ──→ [consumer] ──→ [Postgres: account_balance, outcomes, aml_alerts]
                              │                 ▲
                              └─ métricas → [Prometheus] → [Grafana]
[baseline batch determinístico] ──→ reconcile ─┘ (diff stream × batch)
```

Componentes e containers:
- **kafka** (KRaft, 1 nó) + **kafka-init** (cria `tx.raw`, `tx.outcomes`, `aml.alerts`).
- **flink-jobmanager** / **flink-taskmanager** (escalável) — imagem custom com
  PyFlink 1.19, conector Kafka, plugin S3 e reporter Prometheus; **job-submitter**
  submete os jobs e encerra.
- **minio** (+setup) — checkpoints/estado RocksDB (`s3://pix`).
- **postgres** — `account_balance` (saldo final), `outcomes`, `aml_alerts`, `metrics`.
- **producer** / **consumer** (imagem Python leve) — ingestão e materialização+métricas.
- **prometheus** + **grafana** — dashboard *Pix Settlement Engine* (TPS, latência
  P50/95/99, fila AML, tamanho/duração de checkpoint).

## 5. Workloads evaluated

- **[W-SETTLE]** Liquidação por transação: validação → ledger keyed por conta em
  ordem de event-time → débito/crédito → `SETTLED`/`REJECTED` (`settlement_job.py`).
- **[W-AML]** Detecção de lavagem: ciclo `A→B→C→A`, *structuring* e velocidade
  (`aml_job.py`).
- **[W-RECON]** Reconciliação financeira: saldo final do stream × baseline batch
  determinístico (`baseline.py` + `reconcile.py`).
- **[W-BURST]** Carga com bursts aleatórios: picos de volume (10–80×) e injeção de
  transações sintéticas de alto valor (`producer/burst.py`).

Experimentos (nomeados W1–W9) variam **paralelismo**, **garantia**, **intervalo de
checkpoint** e **bursts**, medindo throughput, latência, recovery, reconciliação e
qualidade do AML.

## 6. Experiments and results

> **MANDATÓRIO**: ≥3 repetições por configuração, com média e desvio padrão.

### 6.1 Experimental environment

Preencher com a máquina usada, por exemplo:
> VM com N vCPUs, M GB RAM, Ubuntu 22.04, Docker 27.x, Docker Compose v2.x.

### 6.2 How to run the benchmarks

```bash
# W1 (throughput×paralelismo), W3 (EO×ALO), W6 (custo de checkpoint), ≥3 reps:
REPS=3 MAX_RECORDS=200000 ./bin/run_experiments.sh
# -> results/runs.csv (por run), results/agg.csv (média±desvio), results/plot_*.png

# W5 reconciliação (EO=zero; ALO+falha≠zero):
./bin/reconcile.sh
# W4 recovery: ./bin/kill_taskmanager.sh durante uma execução e cronometre o retorno.
# W7 AML precision/recall e W9 bursts saem de results/runs.csv (collect.py).
```

O harness reinicia a stack a cada repetição (isolamento) e usa `MAX_RECORDS` para
manter o tempo viável; `aggregate.py` calcula média e desvio (ddof=1) e `plots.py`
gera barras com *error bars*.

### 6.3 What was tested

| ID | Variável | Métricas |
|----|----------|----------|
| W1 | paralelismo (1/2/4[/8/16]) | throughput (TPS) |
| W2 | carga fixa | latência P50/P95/P99 |
| W3 | EXACTLY_ONCE × AT_LEAST_ONCE | TPS, latência |
| W4 | `docker kill` do TM a ~30%/70% | tempo de recovery |
| W5 | EO × ALO+falha | diff saldo stream×batch |
| W6 | checkpoint 1s/5s/30s | latência, tamanho de estado, recovery |
| W7 | — | precision/recall AML vs `isFraud` |
| W8 | TTL on/off | crescimento do estado RocksDB |
| W9 | bursts on/off + intensidade | pico de latência, recovery, diff |

### 6.4 Results

> As tabelas abaixo são **modelos** — preencha com os números de `results/agg.csv`
> após executar o harness no ambiente final. Todos os valores devem trazer
> **média ± desvio padrão** de no mínimo 3 execuções.

**W1 — Throughput por paralelismo**

| Workload | Config | TPS (média) | Desvio | Runs |
|---|---|---|---|---|
| W-SETTLE | parallelism=1 | _ | _ | 3 |
| W-SETTLE | parallelism=2 | _ | _ | 3 |
| W-SETTLE | parallelism=4 | _ | _ | 3 |

**W3 — Custo do exactly-once**

| Config | TPS (média±dp) | Latência P95 (média±dp) | Runs |
|---|---|---|---|
| EXACTLY_ONCE | _ | _ | 3 |
| AT_LEAST_ONCE | _ | _ | 3 |

**W5 — Reconciliação financeira** (resultado esperado)

| Modo | Falha injetada | Σ\|diff\| (centavos) |
|---|---|---|
| EXACTLY_ONCE | sim (kill TM) | **0** |
| AT_LEAST_ONCE | sim (kill TM) | **> 0** (double-count) |

**W6 — Custo de checkpoint**

| Intervalo | Latência P95 | Tamanho do checkpoint | Recovery |
|---|---|---|---|
| 1 s | _ | _ | _ |
| 5 s | _ | _ | _ |
| 30 s | _ | _ | _ |

**W7 — AML precision/recall** (de `collect.py`, nível de conta vs `isFraud`): _ / _.

Inclua os gráficos `results/plot_tps.png` e `results/plot_lat_p95.png` (com barras
de erro) e discuta as tendências (escalabilidade ~linear até saturar, sobrecusto
do exactly-once, trade-off do intervalo de checkpoint, etc.).

## 7. Limitations and conclusions

- **Semântica simplificada do ledger**: saldo inicial constante por conta (em vez
  dos saldos por linha do PaySim, inconsistentes entre linhas) e ops aplicadas
  independentemente (um débito rejeitado não cancela o crédito pareado). Ambos os
  mundos (stream e batch) usam a **mesma** regra, então a reconciliação permanece
  exata — o objetivo do experimento de exactly-once é preservado.
- **Bursts e reconciliação**: o modo `synthetic` injeta transações de alto valor em
  contas `BURST*`, **excluídas** da reconciliação; a prova financeira usa o modo
  `rate` (replay determinístico) ou bursts apenas de taxa.
- **AML**: a detecção de ciclo roda com paralelismo 1 (grafo recente em memória,
  podado por event-time) — adequado para a fração `TRANSFER`, mas é o ponto de
  contenção sob volume muito alto. Precision/recall são avaliados a nível de conta.
- **Reprodutibilidade**: event-time e ordenação são função pura do input, o que
  torna o resultado exactly-once idêntico ao baseline — base da demo de falha.

Conclusão: o projeto demonstra de forma visual e mensurável o **valor do
exactly-once** — o sistema cai, recupera do último checkpoint e o saldo final bate
ao centavo com o baseline determinístico, enquanto o modo at-least-once diverge.

## 8. References and external resources

- PaySim dataset (Kaggle): <https://www.kaggle.com/datasets/ealaxi/paysim1>
- Apache Flink 1.19 (DataStream, State, Checkpointing): <https://flink.apache.org/>
- PyFlink: <https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/python/overview/>
- Apache Kafka (KRaft): <https://kafka.apache.org/>
- MinIO: <https://min.io/> · PostgreSQL: <https://www.postgresql.org/>
- Prometheus: <https://prometheus.io/> · Grafana: <https://grafana.com/>
- IBM TabFormer (dataset alternativo): <https://github.com/IBM/TabFormer>
