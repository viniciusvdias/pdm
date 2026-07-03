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

A aplicação inteira está dockerizada. Pré-requisito: Docker e Docker Compose instalados na máquina.

### Serviços do Compose
- `kafka` — broker (KRaft, sem ZooKeeper). Acessível em `localhost:9092` (host) e `kafka:29092` (containers).
- `collector` — coletor da Binance (Python nativo).
- `processor` — processador analítico (Spark Structured Streaming).
- `jupyter` — ambiente SciPy isolado para análise de dados e geração de gráficos.

Para uma execução livre (fora da bateria de testes), basta rodar:
```bash
docker compose up --build
```

---

## Experimentos e análise de desempenho

Os experimentos avaliam a escalabilidade horizontal e o trade-off entre retenção e latência, variando a seguinte grade:
- **Tamanho da janela:** 15, 30 e 60 segundos.
- **Partições do Kafka:** 1, 5 e 10 partições.
- **Duração por rodada:** 16 minutos (960 segundos) para acumular histórico suficiente para os indicadores (mínimo de 14 candles para o RSI).
- **Repetições:** 3 rodadas completas (intercaladas para capturar a diferença de volatilidade temporal do mercado).

> ⚠️ **Atenção:** Devido à combinação de 9 configurações × 3 repetições × 16 minutos, a execução completa da bateria leva aproximadamente 7,2 horas ininterruptas.

### Executando a coleta

```bash
# 1. Garanta que não há nenhum container rodando e bloqueando o tópico
docker compose down

# 2. Inicie o script orquestrador (ele subirá o Kafka automaticamente)
./bin/experiment.sh
```

O script salvará o estado de cada execução gerando os arquivos `metrics/performance_p<P>_w<W>_r<R>.csv` e os respectivos candles. O horário exato do teste é registrado nos CSVs para correlacionar a vazão (throughput) com a agitação do mercado financeiro naquele momento.

### Analisando os Resultados (Jupyter)

Toda a análise estatística (média ± desvio-padrão) e a plotagem dos gráficos com barras de erro também são feitas via container.

> **⚠️ Permissões:** Como os arquivos CSV e a pasta de métricas são gerados pelo container do Spark (que processa os dados como `root`), o container do Jupyter (que roda com usuário restrito) pode apresentar erro ao tentar salvar os gráficos finais. Para liberar o acesso de escrita, execute este comando na raiz do projeto antes de abrir o notebook:
> ```bash
> docker run --rm -v "$(pwd)/metrics:/app/metrics" python:3.12-slim chmod -R 777 /app/metrics
> ```

```bash
# Suba o ambiente analítico em segundo plano
docker compose up jupyter
```

No final do log, copie a URL gerada (ex: `http://127.0.0.1:8888/lab?token=...`) e abra no seu navegador. Navegue até o arquivo `analysis.ipynb` e execute as células. As tabelas resumo serão impressas e os gráficos `.png` aparecerão automaticamente na sua pasta `metrics/` local.

## Estrutura do repositório

```text
/
├── bin/
│   └── experiment.sh           # Script orquestrador da bateria de experimentos
├── metrics/                    # CSVs, gráficos e métricas geradas
├── src/
│   ├── analysis.ipynb          # Análise estatística e visualização dos resultados
│   ├── binance.py              # Coletor/producer Kafka a partir da Binance
│   ├── config.py               # Configuração central da aplicação
│   └── processor.py            # Processador Spark (OHLCV + EMA/RSI + métricas)
├── docker-compose.yml          # Definição dos serviços Kafka, collector e processor
├── Dockerfile                  # Imagem da aplicação Python+Spark
├── README.md                   # Documentação do projeto
└── requirements.txt            # Dependências Python
```

## Resultados

Os resultados estatísticos brutos (média ± desvio padrão) extraídos das 3 rodadas de cada configuração estão consolidados na tabela abaixo:

| Partições | Janela (s) | Rodadas | Vazão Média (trades/s) | Latência p50 Média (ms) | Latência p95 Média (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 1 | 15 | 3 | 96.6 ± 41.3 | 37056.7 ± 2750.6 | 37091.5 ± 2805.0 |
| 1 | 30 | 3 | 208.4 ± 123.4 | 56828.1 ± 662.6 | 56828.1 ± 662.6 |
| 1 | 60 | 3 | 426.1 ± 457.4 | 102684.7 ± 1168.1 | 102684.7 ± 1168.1 |
| 5 | 15 | 3 | 118.4 ± 116.5 | 36202.3 ± 847.7 | 36229.7 ± 885.4 |
| 5 | 30 | 3 | 236.7 ± 100.2 | 57749.6 ± 859.1 | 57749.6 ± 859.1 |
| 5 | 60 | 3 | 382.4 ± 206.6 | 104432.1 ± 2406.8 | 104432.1 ± 2406.8 |
| 10 | 15 | 3 | 98.4 ± 71.7 | 37693.7 ± 1717.4 | 37819.8 ± 1749.1 |
| 10 | 30 | 3 | 128.1 ± 51.4 | 57418.0 ± 302.4 | 57418.0 ± 302.4 |
| 10 | 60 | 3 | 288.6 ± 47.8 | 104679.7 ± 1853.4 | 104679.7 ± 1853.4 |

> *(Os gráficos gerados pela análise estatística devem ser inseridos nesta seção para visualização das métricas consolidadas: `throughput_comparison.png`, `latency_distribution.png`, `latency_timeline.png` e `stats_error_bars.png`)*

### Discussões

#### Escalabilidade e Vazão
A análise dos dados revela uma forte correlação positiva entre o tamanho da janela de agregação e a vazão do sistema. Janelas de 60s amortizam significativamente o custo computacional, alcançando as maiores vazões médias (até 426.1 trades/s com 1 partição). 

No entanto, escalar horizontalmente (aumentando o número de partições) não resultou em ganhos lineares de vazão. Observa-se que 1 e 5 partições apresentaram resultados próximos, enquanto 10 partições causaram uma **queda** na vazão em janelas maiores (de 382 trades/s em p=5 para 288 trades/s em p=10, na janela de 60s).

#### Trade-off de Latência e Gargalo Identificado
O gargalo principal identificado na arquitetura local é o *overhead* de coordenação e concorrência pela CPU. Como o teste foi executado em um processador de 10 núcleos (i5-13450HX), forçar 10 partições concorrentes para leitura, processamento no Spark e gerenciamento do broker Kafka resultou em *context switching* excessivo, prejudicando a vazão. 

A latência também se comportou de maneira previsível em relação às janelas: janelas maiores implicam retenção de estado por mais tempo, elevando a latência basal. O desvio padrão expressivo (barras de erro) na vazão, especialmente na configuração de 1 partição com janela de 60s, sugere uma alta sensibilidade à volatilidade natural do mercado de criptomoedas durante a execução das rodadas de teste.

## Conclusão

A arquitetura de *stream processing* estruturado demonstrou resiliência no processamento contínuo, sem vazamentos ou falhas sob o volume de dados crus da Binance. O estudo comprova que, para implantações locais ou em *single-nodes*, o "sweet spot" de configuração se encontra ao redor de 5 partições com janelas de 30 a 60 segundos. O excesso de paralelismo (10 partições em 10 núcleos) age como um antipadrão no ambiente de teste, aumentando o custo de sincronização e trocas de contexto sem trazer benefícios de vazão, limitando assim a escalabilidade vertical da máquina.