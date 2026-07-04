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

- Origem: Kaggle — <https://www.kaggle.com/datasets/mtalaltariq/paysim-data/data>
- ~6,36 milhões de transações, ~470 MB (CSV).
- Schema: `step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest,
  oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud`.
- Tipos: `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER`. Apenas `TRANSFER`
  é conta→conta (`C...`→`C...`), o que viabiliza o padrão de lavagem circular.
- Rótulo `isFraud` usado para avaliar precision/recall do AML.


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

## 4. Project architecture

> Diagramas detalhados (mermaid) da arquitetura, do fluxo de dados e da
> demonstração de falha: [`presentation/ARQUITETURA.md`](presentation/ARQUITETURA.md).

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

### 4.1 Paralelismo

No Flink, **paralelismo** é o número de cópias (subtarefas) de um operador que rodam
ao mesmo tempo, cada uma processando uma fatia dos dados. Em vez de um único
processo computando todas as transações em série, o trabalho é dividido entre várias
subtarefas que executam em paralelo, em *task slots* distintos do TaskManager. É o
principal eixo de escala horizontal do projeto, avaliado no experimento W1.

O paralelismo default do job é lido da variável
`PARALLELISM` (`env.set_parallelism(...)` em `flink_common.py`), e cada TaskManager
oferece um número fixo de *task slots* (`TASK_SLOTS`, 4 no ambiente dos experimentos)
que limita quantas subtarefas rodam de fato em paralelo na mesma máquina. Aumentar o
paralelismo sem slots ou TaskManagers suficientes não traz ganho.

O paralelismo só é correto porque
o estado é **particionado por conta**. O `settlement_job` faz `key_by(account)` antes
do `Ledger`, então cada conta é roteada sempre para a mesma subtarefa. Assim cada
subtarefa é dona de um subconjunto disjunto de contas e mantém, isolado, o saldo e o
buffer de ops dessas contas. Como as contas são independentes entre si (o saldo de
uma não depende do saldo de outra), processá-las em paralelo não altera o resultado;
a ordem que importa é a ordem **por conta**, e essa é preservada dentro de cada
subtarefa.

No `aml_job`, os padrões
VELOCITY e STRUCTURING são *keyed* por conta de origem e portanto paralelizáveis. Já
a detecção de ciclo (`A→B→C→A`) precisa enxergar o **grafo global** de transferências
recentes para achar o caminho que fecha o ciclo; se as arestas fossem espalhadas por
várias subtarefas, nenhuma teria a visão completa. Por isso ela é forçada a
paralelismo 1 (uma única subtarefa mantém o grafo recente em memória, podado por
event-time). Esse é o ponto de contenção do AML sob volume muito alto, documentado na
§7.

Mais paralelismo aumenta o throughput e reduz a latência,
porque o backlog drena mais rápido, mas o ganho é **sublinear**. Os números estão na §6.4.

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

Resultados desta seção medidos em:
> Host Windows 11 Pro, Docker Desktop com backend WSL2; **16 vCPUs** e **~15,6 GB
> de RAM** alocados ao Docker. Docker **27.2.0**, Docker Compose **v2.29.2**.
> Toda a stack roda em containers Linux. **1 TaskManager** com **4 task slots**
> (`TASK_SLOTS=4`), estado em RocksDB e checkpoints no MinIO (`s3://pix`).

- **Dataset:** PaySim amplificado ×3 (`data/paysim_x3.csv`, **1,5 GB**,
  **19.087.860** registros; ver §2.2).
- **Workload por run:** os primeiros `MAX_RECORDS=10000` registros do dataset, que
  expandem em **12.242 ops de ledger** (8.040 débitos SETTLED, 2 REJECTED, 4.200
  créditos). Tamanho escolhido para manter o tempo viável reiniciando a stack a
  cada repetição (≥3 reps por configuração).

### 6.2 How to run the benchmarks

```bash
# W1 (throughput×paralelismo), W3 (EO×ALO), W6 (custo de checkpoint), ≥3 reps:
REPS=3 MAX_RECORDS=10000 ./bin/run_experiments.sh
# -> results/runs.csv (por run), results/agg.csv (média±desvio), results/plot_*.png

# W5 reconciliação (EO=zero; ALO+falha≠zero):
./bin/reconcile.sh
# W4 recovery: ./bin/kill_taskmanager.sh durante uma execução e cronometre o retorno.
# W7 AML precision/recall e W9 bursts saem de results/runs.csv (collect.py).
```

**Como cada métrica é calculada** (reprodutível, sem intervenção manual):

1. O harness reinicia a stack do zero a cada repetição (`docker compose down -v`)
   para isolamento e sobe com a configuração do run (`PARALLELISM`, `GUARANTEE`,
   `CHECKPOINT_MS`), processando os primeiros `MAX_RECORDS` registros (`RATE=0`,
   o mais rápido possível).
2. **Fim do workload (detecção determinística):** o número de ops é função pura do
   input, então o harness calcula o alvo `EXPECTED` (ops do baseline sobre os mesmos
   `MAX_RECORDS`) e espera a contagem de `outcomes` no Postgres **atingir esse alvo**.
   Isso é robusto às duas fontes de atraso desta arquitetura: o sink *exactly-once*
   só torna os outcomes visíveis ao **commitar por checkpoint**, e a cauda
   bufferizada só sai após o **timer de drain**. `elapsed` é medido de t0 (início do
   producer) até o instante em que o alvo é atingido.
3. **Throughput (TPS)** = `outcomes / elapsed` (`collect.py`). É a vazão **ponta a
   ponta** do motor de liquidação (ops de ledger por segundo), e não a taxa de
   ingestão bruta no Kafka (o producer publica ~15 k registros/s, ver logs).
4. **Latência P50/P95/P99** = percentis de `settle_time_ms − ingest_time_ms` sobre
   todos os outcomes do run (relógio de parede ponta a ponta). Ver a nota em §6.4
   sobre o que essa latência representa nesta carga.
5. `aggregate.py` calcula **média e desvio padrão amostral (ddof=1)** por
   configuração; `plots.py` gera barras com *error bars* (`results/plot_*.png`).

> Nota de metodologia: ao habilitar este harness encontramos e corrigimos dois
> defeitos que o impediam de medir corretamente (ver §7): `MAX_RECORDS` não chegava
> ao container do producer, e a espera fixa pós-producer era curta demais para o
> dreno real. Os números abaixo já usam a versão corrigida.

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

Valores reais de `results/agg.csv` (3 repetições por configuração, **média ±
desvio padrão amostral**, ddof=1). Workload = 12.242 ops por run (§6.1). Throughput
em **ops de liquidação por segundo**; latência ponta a ponta em segundos.

**W1 — Throughput por paralelismo** (`GUARANTEE=EXACTLY_ONCE`, `CHECKPOINT_MS=5000`)

| Workload | Config | TPS (média ± dp) | Runs |
|---|---|---|---|
| W-SETTLE | parallelism=1 | 92,6 ± 1,6 | 3 |
| W-SETTLE | parallelism=2 | 166,7 ± 7,6 | 3 |
| W-SETTLE | parallelism=4 | 273,3 ± 10,4 | 3 |

Escalabilidade **sublinear**: 1→2 dá 1,80× e 2→4 dá 1,64× (2,95× no total para 4×
os slots). O limite é o overhead do operador PyFlink (Python UDF + estado RocksDB)
num único TaskManager; a ingestão no Kafka (~15 k reg/s) não é o gargalo.

**W2 — Latência por paralelismo** (mesmos runs do W1; segundos)

| Config | P50 | P95 | P99 |
|---|---|---|---|
| parallelism=1 | 65,8 ± 0,8 | 122,0 ± 0,7 | 125,2 ± 0,8 |
| parallelism=2 | 40,7 ± 1,7 | 64,2 ± 2,8 | 65,7 ± 2,4 |
| parallelism=4 | 19,3 ± 1,5 | 34,1 ± 1,4 | 36,0 ± 1,4 |

A latência cai com o paralelismo porque o **mesmo backlog drena mais rápido**. Veja
a nota abaixo sobre o que essa latência representa.

**W3 — Custo do exactly-once** (`parallelism=2`)

| Config | TPS (média ± dp) | Latência P95 (s) | Runs |
|---|---|---|---|
| EXACTLY_ONCE | 173,8 ± 1,0 | 61,3 ± 0,6 | 3 |
| AT_LEAST_ONCE | 182,6 ± 1,9 | 60,0 ± 1,6 | 3 |

O exactly-once custa só **~5 % de throughput** e latência praticamente igual — o
gargalo é o operador de liquidação, não as transações do sink Kafka. Ou seja, a
garantia forte sai quase de graça nesta carga.

**W5 — Reconciliação financeira** (saldo final do stream × baseline batch)

| Modo | Workload | Σ\|diff\| (centavos) |
|---|---|---|
| EXACTLY_ONCE | amostra (13.964 reg) | **0** ✔ (verificado) |
| EXACTLY_ONCE | fatia 10k (mesma dos experimentos) | **0** ✔ (verificado) |

Sob exactly-once o saldo de **todas** as contas (16.835 na amostra; 10.669 na fatia
de 10k) bate **ao centavo** com o baseline determinístico. A divergência esperada do
modo at-least-once **sob falha injetada** (kill de TaskManager, W4) é o
comportamento previsto na §7, mas não foi cronometrada neste ambiente.

**W6 — Custo do intervalo de checkpoint** (`parallelism=2`, `EXACTLY_ONCE`)

| Intervalo | TPS (média ± dp) | Latência P95 (s) | Runs |
|---|---|---|---|
| 1 s | 170,3 ± 5,3 | 62,2 ± 0,6 | 3 |
| 5 s | 168,0 ± 10,4 | 63,5 ± 1,8 | 3 |
| 30 s | 157,6 ± 0,5 | 62,7 ± 0,6 | 3 |

De 1 s para 5 s o impacto é desprezível; em 30 s o throughput cai ~8 %. Como o sink
exactly-once só **commita os outcomes a cada checkpoint**, um intervalo grande atrasa
a visibilidade do último lote e infla o tempo ponta a ponta. A latência P95 quase não
muda (o atraso concentra-se na cauda final).

**W7 — AML precision/recall** (nível de conta vs `isFraud`): **0 / 0** neste
subconjunto (135 contas com `isFraud=1`, **0 alertas**). Os primeiros 10 k registros
(início do shard 0) não têm origem repetida nem ciclos suficientes para cruzar os
limiares de VELOCITY/STRUCTURING/CYCLE — cada conta de origem aparece ~1 vez. A
detecção AML só produz sinal com o **dataset completo** (ou limiares ajustados); ver
§7.

Gráficos com barras de erro: `results/plot_tps.png` (throughput por configuração) e
`results/plot_lat_p95.png` (latência P95).

> **O que a "latência" mede aqui.** Não é latência de rede por registro. O producer
> despeja os 10 k registros em ~7 s, mas o operador de liquidação processa a ~90–270
> ops/s, formando um **backlog**; cada op acumula o tempo de fila desse backlog + o
> buffering por event-time (espera o watermark) + o gating de commit do
> exactly-once. Por isso a latência é de dezenas de segundos e **cai quando o
> throughput sobe** (P50 de 65,8 s em p=1 para 19,3 s em p=4). É a latência ponta a
> ponta do motor sob rajada, coerente com a §7.

## 7. Limitations and conclusions

- **Semântica simplificada do ledger**: saldo inicial constante por conta (em vez
  dos saldos por linha do PaySim, inconsistentes entre linhas) e ops aplicadas
  independentemente (um débito rejeitado não cancela o crédito pareado). Ambos os
  mundos (stream e batch) usam a **mesma** regra, então a reconciliação permanece
  exata.
- **Bursts e reconciliação**: o modo `synthetic` injeta transações de alto valor em
  contas `BURST*`, **excluídas** da reconciliação; a prova financeira usa o modo
  `rate` (replay determinístico) ou bursts apenas de taxa.
- **AML**: a detecção de ciclo roda com paralelismo 1 (grafo recente em memória,
  podado por event-time), adequado para a fração `TRANSFER`, mas é o ponto de
  contenção sob volume muito alto. Precision/recall são avaliados a nível de conta.
  **Nos experimentos (subconjunto de 10 k) o AML não emitiu alertas** (cada origem
  aparece ~1 vez): a avaliação de precision/recall exige o dataset completo ou
  limiares ajustados (W7).
- **Throughput modesto do operador de liquidação**: a vazão ponta a ponta fica em
  ~90–270 ops/s (§6.4), limitada pelo overhead do operador PyFlink (Python UDF +
  buffering por event-time + estado RocksDB) num único TaskManager. A ingestão no
  Kafka é muito maior (~15 k reg/s). O ganho com paralelismo é claro mas sublinear;
  escalar exigiria mais TaskManagers e/ou reduzir o custo por elemento. A "latência" medida reflete a fila desse backlog sob rajada,
  não latência de rede.
- **Defeitos encontrados e corrigidos ao habilitar os benchmarks** (a suíte não
  media corretamente antes): (1) `MAX_RECORDS` não era repassado ao container do
  producer no `docker-compose.yml`, de modo que todo run processava o dataset
  inteiro; (2) a espera pós-producer no harness era um `sleep` fixo curto demais
  para o dreno real do settlement (o sink exactly-once só commita por checkpoint e a
  cauda só sai no timer de drain), então as métricas pegavam um instante transitório.
  O harness passou a esperar a contagem de outcomes atingir o alvo determinístico
  (`EXPECTED`). Os resultados da §6 já usam a versão corrigida.
- **Reprodutibilidade**: event-time e ordenação são função pura do input, o que
  torna o resultado exactly-once idêntico ao baseline, base da demo de falha.

Conclusão: o projeto demonstra de forma visual e mensurável o **valor do
exactly-once**, o sistema cai, recupera do último checkpoint e o saldo final bate
ao centavo com o baseline determinístico, enquanto o modo at-least-once diverge.

## 8. References and external resources

- PaySim dataset (Kaggle): <https://www.kaggle.com/datasets/ealaxi/paysim1>
- Apache Flink 1.19 (DataStream, State, Checkpointing): <https://flink.apache.org/>
- PyFlink: <https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/python/overview/>
- Apache Kafka (KRaft): <https://kafka.apache.org/>
- MinIO: <https://min.io/> · PostgreSQL: <https://www.postgresql.org/>
- Prometheus: <https://prometheus.io/> · Grafana: <https://grafana.com/>
- IBM TabFormer (dataset alternativo): <https://github.com/IBM/TabFormer>
