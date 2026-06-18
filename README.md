# Pipeline de Streaming de Trades — Binance → Kafka → Spark

Projeto final da disciplina de Big Data. Um **pipeline de processamento de dados em
tempo real** que ingere negócios (trades) do mercado de criptomoedas, agrega-os em
candles OHLCV por janela temporal, calcula indicadores técnicos (EMA, RSI) e avalia
o desempenho do sistema sob diferentes níveis de paralelismo.

---

## 1. Fonte de dados

- **O quê:** stream de trades em tempo real do par **BTC/USDT** da Binance.
- **De onde:** WebSocket público da Binance — `wss://stream.binance.com:9443/ws/btcusdt@trade`
  ([documentação](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)).
- **Formato de entrada:** mensagens JSON, uma por trade, empurradas continuamente pela Binance.

Cada trade traz preço, quantidade, identificador único (`trade_id`), timestamp e o lado
agressor (compra/venda).

## 2. Workload (processamento)

Sobre esse fluxo, o pipeline executa **processamento de streaming com janelas temporais**:

1. **Ingestão e normalização** dos trades crus para um schema único.
2. **Agregação em janelas** (*window functions*) de N segundos, produzindo candles **OHLCV**
   (Open, High, Low, Close, Volume) — com desempate correto de Open/Close pelo `trade_id`.
3. **Indicadores técnicos** sobre a série de candles: **EMA(9)** e **RSI(14)**.
4. **Instrumentação**: latência fim-a-fim, vazão (trades/s) e completude.

## 3. Por que isso é relevante

Candles e indicadores são a base de praticamente toda análise técnica de mercado. Calcular
isso **em tempo real** é o que permite sistemas de trading, alertas e gestão de risco
reagirem a um mercado que se move em milissegundos — algo impossível com processamento em
lote tradicional (carregar tudo num banco e analisar depois).

## 4. Que tipo de processamento é este?

**Stream processing (processamento de fluxo)**, estruturado, com estado e janelas temporais.
Não é batch: os dados são ilimitados e processados conforme chegam, com *watermark* para
lidar com eventos atrasados.

## 5. Por que isso é Big Data?

O enunciado considera dado válido aquele que exige **≥ 1 GB de armazenamento e/ou é caro de
processar**. Nosso argumento **não é volume estático, é velocidade e volume não-limitado**:

- **Velocidade (Velocity):** o stream é contínuo e de alta frequência. Medimos picos de
  **~90–100 trades/s** só no BTC/USDT. São centenas de milhares de eventos por hora, sem fim.
- **Volume não-limitado:** um fluxo que nunca "termina". Persistindo os trades crus, o
  dataset cresce indefinidamente e ultrapassa 1 GB em poucas horas de coleta.
- **Caro de processar:** agregar com janelas e estado, sob latência baixa e de forma
  escalável, exige uma stack de streaming distribuído (Kafka + Spark) — não cabe num
  `pandas.read_csv`.

As 3 V's clássicas aparecem: **Volume** (acumulação contínua), **Velocidade** (tempo real),
**Veracidade** (dados crus que exigem ordenação/desempate por `trade_id`).

## 6. Arquitetura

```
   ┌──────────────┐     JSON      ┌─────────────────┐    JSON     ┌──────────────────────┐    CSV     ┌──────────────┐
   │  1. Coletor  │  ──────────▶  │  2. Kafka       │  ────────▶  │  3. Processador      │  ───────▶  │ 4. Análise   │
   │  binance.py  │   trade msg   │  tópico `trades`│   consume   │  Spark Structured    │  métricas  │ analysis     │
   │ (WebSocket)  │   key=symbol  │  1..N partições │             │  Streaming           │  + candles │ .ipynb       │
   └──────────────┘               └─────────────────┘             │  (OHLCV+EMA+RSI)     │            │ (gráficos)   │
                                                                  └──────────────────────┘            └──────────────┘
```

| Componente | Tecnologia | Papel | Formato |
|---|---|---|---|
| Coletor | Python + `websockets` + `confluent-kafka` | Ingestão e normalização; produz no Kafka com reconexão | JSON |
| Fila | Apache Kafka (KRaft) | Desacopla produtor/consumidor; particionamento p/ paralelismo | JSON |
| Processador | Apache Spark Structured Streaming | Janelamento OHLCV, indicadores, métricas | CSV (saída) |
| Análise | Jupyter + pandas + matplotlib | Estatística e gráficos dos experimentos | PNG/CSV |

**Schema da mensagem (contrato entre os módulos):**

```json
{"trade_id": 6419174977, "ts_trade": 1781746345543, "ts_ingest": 1781746345641,
 "price": "64723.98", "qty": "0.00328", "side": "sell", "symbol": "BTCUSDT"}
```

- `open` = preço do trade de **menor** `trade_id` na janela (`min_by`)
- `close` = preço do trade de **maior** `trade_id` na janela (`max_by`)

---

## Como rodar (Docker Compose)

A aplicação inteira está dockerizada. Pré-requisito: Docker + Docker Compose.

```bash
# Sobe Kafka + Coletor + Processador. Os candles e métricas saem em ./metrics
docker compose up --build
```

Na primeira execução o Spark baixa o conector Kafka (alguns minutos). Os candles OHLCV
e indicadores aparecem no log do serviço `processor` e os CSVs em `metrics/`.

Para parar: `Ctrl+C` e depois `docker compose down`.

### Serviços
- `kafka` — broker (KRaft, sem ZooKeeper). Acessível em `localhost:9092` (host) e `kafka:29092` (containers).
- `collector` — coletor da Binance.
- `processor` — processador Spark.

---

## Experimentos e análise de desempenho

Os experimentos variam o **número de partições do Kafka** (1, 2, 4) e o **tamanho da janela**,
com **múltiplas repetições por configuração** (mínimo 3) para reportar **média ± desvio-padrão**.

```bash
# Sobe só o Kafka...
docker compose up -d kafka

# ...e roda a bateria de experimentos (host driver, mesmo código do processor.py)
./experiment.sh                      # todas as configs, 3 repetições, 120s cada
REPS=3 EXPERIMENTS="1_10 2_10 4_10" ./experiment.sh   # só a varredura de partições
```

Cada rodada gera `metrics/performance_p<P>_w<W>_r<R>.csv`. Em seguida:

```bash
.venv/bin/jupyter notebook analysis.ipynb   # gera tabelas e gráficos
```

O notebook produz:
- **Tabela** com vazão e latência em **média ± desvio-padrão** por configuração.
- **Gráficos com barras de erro**: vazão e latência p95 por nº de partições.
- Candlestick com EMA/RSI e timeline de latência.

> Nota: os experimentos são orquestrados a partir do host (`.venv`) por conveniência de
> medição (controle de tempo e variáveis por rodada), mas executam **o mesmo `processor.py`**
> que roda no Docker Compose.

---

## Estrutura do repositório

```
binance.py        # Coletor (producer Kafka)
processor.py      # Processador Spark (OHLCV + EMA/RSI + métricas)
consumer.py       # Consumidor simples (validação manual do tópico)
config.py         # Configuração central (lida do .env)
docker-compose.yml# Kafka + collector + processor
Dockerfile        # Imagem da aplicação Python (coletor/processador)
experiment.sh     # Bateria de experimentos com repetições
analysis.ipynb    # Análise estatística e gráficos
metrics/          # CSVs e gráficos gerados (não versionado)
```

## Resultados

> Preencher após rodar `./experiment.sh` e o `analysis.ipynb`. A tabela de
> `metrics/summary_stats.csv` e os PNGs em `metrics/` entram aqui, seguidos da discussão:
> como a vazão escalou com as partições, o trade-off de latência, e o gargalo identificado.

## Conclusão

> Preencher: síntese dos achados de desempenho e limitações.
