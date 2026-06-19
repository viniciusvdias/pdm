# Arquitetura do Projeto (diagramas)

Motor de Liquidação estilo Pix com Apache Flink (PyFlink) · Grupo G3.

Diagramas mermaid da arquitetura, do nível mais alto ao detalhe de cada bloco.
Renderizam no GitHub, no VS Code (extensão Markdown Preview Mermaid) e em
`mermaid.live`.

---

## 1. Visão geral (os dois caminhos a partir do mesmo input)

```mermaid
flowchart LR
    CSV[("PaySim CSV")]

    PROD["producer<br/>lê CSV e publica JSON"]
    RAW{{"Kafka: tx.raw"}}

    subgraph FLINK["Apache Flink (PyFlink)"]
        SETTLE["settlement_job<br/>liquidação por conta"]
        AML["aml_job<br/>detecção de lavagem"]
    end

    OUT{{"Kafka: tx.outcomes"}}
    ALERT{{"Kafka: aml.alerts"}}
    CONS["consumer<br/>materializa + métricas"]
    PG[("Postgres")]
    BASE["baseline batch<br/>fonte da verdade"]
    REC{"reconcile<br/>diff == 0 ?"}
    MINIO[("MinIO<br/>checkpoints RocksDB")]
    PROM["Prometheus"]
    GRAF["Grafana"]

    CSV --> PROD --> RAW
    RAW --> SETTLE --> OUT --> CONS --> PG
    RAW --> AML --> ALERT --> CONS
    SETTLE -. checkpoints .-> MINIO
    CSV --> BASE --> REC
    PG --> REC
    CONS --> PROM --> GRAF
```

Caminho **stream** (tempo real): `CSV -> producer -> Kafka -> Flink -> Kafka ->
consumer -> Postgres`. Caminho **batch** (offline determinístico): `CSV ->
baseline`. No fim, `reconcile` compara os dois: diff zero prova o exactly-once.

---

## 2. Tempo e ordem como função pura do input

```mermaid
flowchart TD
    IN["linha do CSV + índice global"]
    TID["transaction_id<br/>= sha1(linha + índice)"]
    ET["event_time<br/>= BASE_EPOCH + índice*10ms + jitter(tid)"]
    ORD["ordem determinística por conta:<br/>(event_time, transaction_id, kind)"]
    IN --> TID --> ET --> ORD
    IN --> ET
```

Como `event_time` não vem do relógio de execução, a ordem por conta é idêntica no
stream e no batch. É isso que torna o saldo final reprodutível e a prova ao centavo
possível.

---

## 3. Fluxo de dados ponta a ponta (sequência)

```mermaid
sequenceDiagram
    participant CSV as PaySim CSV
    participant P as producer
    participant KR as Kafka tx.raw
    participant F as Flink settlement
    participant KO as Kafka tx.outcomes
    participant C as consumer
    participant DB as Postgres

    CSV->>P: lê linha, normaliza
    P->>KR: JSON + ingest_time_ms (key = transaction_id)
    KR->>F: consome op (event-time + watermark)
    Note over F: buffer por conta,<br/>aplica em ordem de event-time
    F->>KO: outcome (status, balance_cents, apply_seq)
    KO->>C: consome outcome
    C->>DB: upsert saldo (maior apply_seq) + trilha
    Note over C: expõe latência e contadores<br/>em /metrics (Prometheus)
```

---

## 4. Dentro do job de liquidação (`settlement_job`)

```mermaid
flowchart TD
    SRC["KafkaSource tx.raw<br/>+ watermark (event-time)"]
    FM["flat_map (expand_json)<br/>transação -> ops por conta"]
    KEY["key_by(account)"]
    LED["Ledger<br/>(KeyedProcessFunction)"]
    SINK["KafkaSink tx.outcomes<br/>EXACTLY_ONCE | AT_LEAST_ONCE"]
    SRC --> FM --> KEY --> LED --> SINK

    subgraph ESTADO["Estado por conta (RocksDB)"]
        B["balance (ValueState)"]
        BUF["buffer de ops (ListState)"]
        AP["applied / apply_seq (ValueState)"]
    end
    LED -.-> ESTADO
    LED -. checkpoints incrementais .-> MIN[("MinIO s3://pix")]
```

### Ciclo de vida de uma op dentro do Ledger

```mermaid
sequenceDiagram
    participant K as Kafka tx.raw
    participant L as Ledger (por conta)
    participant S as State (RocksDB)
    participant O as Kafka tx.outcomes

    K->>L: op (account, event_time, ...)
    L->>S: buffer.add(op)
    L->>L: registra timer event-time
    Note over L: aguarda watermark passar
    L->>L: timer dispara: ordena ops maduras<br/>por (event_time, tid, kind)
    L->>S: aplica DEBIT/CREDIT, atualiza balance e apply_seq
    L->>O: outcome (status, balance_cents, apply_seq)
    Note over L: ao fim, timer de drain<br/>(processing-time) esvazia a cauda
```

---

## 5. Expansão de uma transação em ops do ledger

```mermaid
flowchart LR
    T["Transaction"]
    CI["CASH_IN"] -->|+valor| C1["CREDIT origem"]
    PD["PAYMENT / DEBIT"] -->|-valor| D1["DEBIT origem"]
    TC["TRANSFER / CASH_OUT"] -->|-valor| D2["DEBIT origem"]
    TC -->|+valor| C2["CREDIT destino"]
    T --> CI
    T --> PD
    T --> TC
```

Regra de aceitação: **DEBIT só liquida se `valor <= saldo`** (senão `REJECTED`);
**CREDIT sempre soma**.

---

## 6. Detecção de lavagem (`aml_job`)

```mermaid
flowchart TD
    EV["eventos tx.raw"]
    subgraph VS["VelocityStructuring (keyed por origem)"]
        VEL["VELOCITY: N tx da mesma origem<br/>em janela curta"]
        STR["STRUCTURING: N transfers<br/>just-below-limit na mesma origem"]
    end
    subgraph CY["CycleDetector (paralelismo 1)"]
        CYC["CYCLE: ciclo A->B->C->A<br/>via arestas TRANSFER (DFS 3 saltos)"]
    end
    EV --> VS
    EV --> CY
    VS --> AL{{"Kafka aml.alerts"}}
    CY --> AL
```

A detecção de ciclo precisa de visão global do grafo recente de transferências, por
isso roda com paralelismo 1. Velocity e structuring são keyed por conta de origem.

---

## 7. Materialização e observabilidade (`consumer`)

```mermaid
flowchart LR
    OUT{{"tx.outcomes"}} --> C["consumer"]
    AL{{"aml.alerts"}} --> C
    C -->|upsert maior apply_seq| AB[("account_balance")]
    C -->|trilha completa| OC[("outcomes")]
    C -->|alertas| AA[("aml_alerts")]
    C -->|latência, contadores| PR["Prometheus /metrics"]
    PR --> GR["Grafana<br/>TPS, P50/P95/P99, fila AML, checkpoints"]
```

---

## 8. A prova final (reconciliação)

```mermaid
flowchart TD
    BCSV[("baseline.csv<br/>saldo batch")]
    PGT[("account_balance<br/>saldo stream")]
    R{"para cada conta:<br/>diff = stream - baseline"}
    BCSV --> R
    PGT --> R
    R -->|soma diff == 0| OK["RECONCILIADO<br/>exactly-once provado"]
    R -->|soma diff != 0| BAD["DIVERGENTE<br/>esperado em at-least-once + falha"]
```

Sob `EXACTLY_ONCE` o resultado é zero. Sob `AT_LEAST_ONCE` com uma falha injetada
(matar um TaskManager), espera-se divergência por double counting, exatamente o que
o experimento quer demonstrar.

---

## 9. Infraestrutura (Docker Compose)

```mermaid
flowchart TD
    KAFKA["kafka (KRaft)"]
    KINIT["kafka-init<br/>cria os 3 tópicos"]
    MINIO["minio"] --> MSETUP["minio-setup<br/>cria bucket pix"]
    PG["postgres<br/>(init.sql)"]
    JM["flink-jobmanager"]
    TM["flink-taskmanager (escalável)"]
    SUB["job-submitter<br/>submete settlement + aml"]
    PROD["producer"]
    CONS["consumer"]
    PROM["prometheus"] --> GRAF["grafana"]

    KAFKA --> KINIT
    MSETUP --> JM
    KAFKA --> JM
    JM --> TM
    JM --> SUB
    KINIT --> SUB
    TM --> SUB
    SUB --> PROD
    KINIT --> PROD
    PG --> CONS
    KINIT --> CONS
```

Estado do Flink em **RocksDB** com checkpoints incrementais no **MinIO**
(`s3://pix/checkpoints`); é daí que vem a recuperação após falha. Garantia,
intervalo de checkpoint, paralelismo e limiares de AML são parametrizados por env
(`.env`), o que permite variar configurações nos experimentos sem tocar no código.

---

## 10. Demonstração de falha e recuperação

```mermaid
sequenceDiagram
    participant U as ./bin/kill_taskmanager.sh
    participant TM as TaskManager
    participant JM as JobManager
    participant CK as MinIO (checkpoints)
    participant R as reconcile

    U->>TM: mata um TaskManager
    Note over JM: detecta falha do job
    JM->>CK: restaura último checkpoint
    JM->>TM: reescalona tarefas (TM remanescente)
    Note over JM,TM: reprocessa desde o checkpoint<br/>sink exactly-once descarta duplicatas
    R->>R: diff stream x baseline
    R-->>U: continua ZERO (ao centavo)
```
