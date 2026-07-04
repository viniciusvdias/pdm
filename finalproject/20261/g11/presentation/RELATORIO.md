# Relatório Técnico — Pipeline de Stream Processing sobre NYC TLC Trip Record Data

**Projeto final · Big Data · Ciência da Computação · UFLA**

> Processamento de fluxo distribuído (stream) de dados reais de corridas de táxi de
> Nova York, com Apache Kafka + Apache Spark Structured Streaming, orquestrado em
> contêineres, avaliado empiricamente em **dois ambientes**: (1) host único local e
> (2) cluster de VMs na AWS. Este relatório documenta motivação, fundamentação,
> arquitetura, implementação, metodologia de avaliação e os resultados das duas
> fases, com análise comparativa.

---

## Sumário

1. **Introdução e Motivação** — o problema, a fonte de dados, por que é Big Data, a
   mudança de batch para stream, objetivos.
2. **Fundamentação Teórica** — batch vs. stream, event time e watermark, windowing,
   micro-batch, pub/sub e Kafka, shuffle e agregações _stateful_.
3. **Arquitetura da Solução** — visão geral do pipeline, o evento canônico,
   componentes e como se conectam.
4. **Implementação** — ingestão (`producer.py`), processamento (`stream_job.py`),
   infraestrutura (`docker-compose.yml`).
5. **Metodologia de Avaliação** — definições operacionais das métricas, coleta,
   desenho experimental (cenários A/B, 3 repetições).
6. **Fase 1 — Experimento Local (host único)** — ambiente, Cenário A, Cenário B,
   discussão.
7. **Fase 2 — Experimento Distribuído (AWS)** — motivação, arquitetura, os seis
   desafios da distribuição, resultados.
8. **Análise Comparativa** — Cenário B lado a lado, o achado central, custo absoluto
   vs. escala, limites remanescentes.
9. **Conclusão** — o que foi demonstrado, limitações, trabalhos futuros.
- **Apêndices** — A. Como reproduzir · B. Glossário de métricas · C. Tabelas
  completas.

---

## 1. Introdução e Motivação

### 1.1. O problema

Metrópoles como Nova York geram, de forma contínua, um volume enorme de eventos de
mobilidade urbana: a cada corrida de táxi ou de carro de aplicativo que começa e
termina, nasce um registro com origem, destino, horário, distância e valor pago.
Esses eventos **não param de chegar** — são um fluxo perpétuo. Sistemas de mobilidade
reais (Uber, 99, táxis convencionais, órgãos de trânsito) precisam responder a
perguntas operacionais **enquanto os dados ainda estão frescos**: quais são os picos
de demanda ao longo do dia, quanto tempo em média uma viagem leva partindo de cada
região da cidade, onde o tráfego está mais congestionado agora. Responder a isso
depois, em lotes noturnos, tarde demais para agir, tem valor operacional muito menor.

Este trabalho constrói e avalia um **pipeline distribuído de processamento de fluxo
(stream)** que consome esses eventos em tempo quase-real, aplica limpeza e filtragem,
e computa duas agregações analíticas — **picos diários de demanda** e **tempo médio
de viagem por zona** — publicando os resultados de forma incremental. O foco não é
apenas produzir os números, mas **medir empiricamente como o pipeline escala** quando
se acrescenta capacidade computacional, em dois ambientes distintos (host único local
e cluster de VMs na AWS).

### 1.2. A fonte de dados

Usamos o **NYC TLC Trip Record Data**, dados públicos e reais da _New York City Taxi
& Limousine Commission_:
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.

A fonte é composta por **quatro serviços distintos**, cada um cobrindo uma modalidade
de transporte da cidade:

| Serviço | Modalidade | Coluna de event-time | Campo de tarifa |
|---|---|---|---|
| `yellow` | táxi amarelo (medallion) | `tpep_pickup_datetime` | `fare_amount` |
| `green` | táxi verde (boro taxi) | `lpep_pickup_datetime` | `fare_amount` |
| `fhv` | for-hire vehicle (aluguel) | `pickup_datetime` | _(ausente)_ |
| `fhvhv` | high-volume FHV (Uber/Lyft) | `pickup_datetime` | `base_passenger_fare` |

Todos os arquivos são distribuídos em **formato Parquet** — um formato colunar,
comprimido e tipado, projetado justamente para análise eficiente de grandes volumes.
O dataset completo publicado pela TLC soma **≈ 102 milhões de linhas / 2,3 GB** em
Parquet. Para manter o experimento reprodutível dentro dos recursos disponíveis,
trabalhamos com um **subconjunto de ≈ 48,8 milhões de linhas / 1,07 GB** (dois meses
de dados, os quatro serviços). Esse número não é arbitrário: é o tamanho do recorte
que baixamos e versionamos no repositório, e é a base sobre a qual todas as métricas
das seções 6–8 foram coletadas.

### 1.3. Por que isto é Big Data

O enunciado da disciplina define como "big data" válido qualquer fonte que exija **ao
menos 1 GB de armazenamento e/ou seja cara de processar** — e pede explicitamente que
o argumento de _por quê_ isto é Big Data faça parte do trabalho. Aqui a justificativa
tem três dimensões, e cada uma pesa de forma independente:

- **Volume.** O subconjunto já ocupa 1,07 GB comprimido em Parquet — e Parquet é um
  formato _comprimido_. Descomprimido em memória, com as colunas materializadas como
  objetos, o dataset é várias vezes maior. As ~48,8M linhas não cabem
  confortavelmente na RAM disponível do host de testes; ler tudo de uma vez estouraria
  a memória. Isso força, por si só, o processamento **em partições/lotes**, e não em
  uma única carga monolítica.

- **Velocidade.** Não tratamos os dados como um arquivo estático a ser lido de uma
  vez, e sim como um **fluxo contínuo de eventos** (um _firehose_): o produtor injeta
  mensagens no sistema a alta vazão, imitando a chegada perpétua de corridas reais. O
  desafio deixa de ser "processar um arquivo" e passa a ser "acompanhar um fluxo que
  não termina", sem acumular _backlog_ e mantendo latência baixa. Esse regime contínuo
  é o que caracteriza cargas _latency-sensitive_.

- **Custo de processamento (shuffle).** Esta é a dimensão mais importante e a que
  realmente exige distribuição. As duas agregações do pipeline — `groupBy` por janela
  de tempo e um `join` com a tabela de zonas — reagrupam registros **por chave**. Num
  sistema distribuído, reagrupar por chave obriga a **redistribuir dados entre os nós
  pela rede**: é o **shuffle** (definido formalmente em §2.5). O shuffle é o gargalo
  clássico do Big Data porque seu custo cresce com o volume _e_ com o número de nós, e
  não é uma operação local — ela paga latência de rede, serialização e coordenação. É
  justamente esse custo que torna o processamento "caro" no sentido do enunciado, e o
  que medimos ao estressar o pipeline.

Em resumo: **volume** (não cabe na RAM de um nó), **velocidade** (fluxo contínuo de
alta vazão) e **custo de shuffle** (agregações que exigem redistribuição pela rede)
justificam, juntos, tratar isto como um problema de Big Data que demanda uma solução
**distribuída**.

### 1.4. A mudança em relação à proposta original: de batch para stream

A proposta original, validada em sala, previa **processamento em batch** sobre o táxi
amarelo. Ao implementar, mudamos para **stream processing** por duas razões:

1. **Adequação ao domínio.** Os dados modelam eventos que "chegam o tempo todo" e cuja
   validade operacional é curta — saber o pico de demanda ou o congestionamento _agora_
   vale mais do que saber ontem à noite. Esse é exatamente o perfil de aplicação
   _latency-sensitive_ discutido na disciplina, para o qual o paradigma correto é
   stream, não batch. Stream processa **incrementalmente**, entregando resultados em
   tempo quase-real sem reprocessar o histórico inteiro a cada rodada.

2. **Realidade dos dados.** Ao inspecionar os quatro serviços, descobrimos que eles
   têm **schemas heterogêneos** — inclusive colunas de event-time com nomes diferentes
   (`tpep_pickup_datetime`, `lpep_pickup_datetime`, `pickup_datetime`) e nem todos com
   campo de tarifa. Um pipeline de stream sobre um **evento canônico** unificado (§3.2)
   se mostrou a forma natural de reconciliar essa heterogeneidade sob um único
   _watermark_ e uma única lógica de processamento — uma decisão de engenharia de dados
   que a proposta batch original, restrita a um só serviço, não precisava enfrentar.

### 1.5. Objetivos do trabalho

O que este trabalho se propõe a demonstrar:

1. **Que o problema é genuinamente Big Data** e que a arquitetura escolhida (Kafka +
   Spark Structured Streaming, em contêineres) é adequada a ele.
2. **Que dados reais heterogêneos podem ser unificados** por um evento canônico sem
   perder a semântica de event-time necessária ao processamento de stream.
3. **Que o pipeline escala com o hardware** — e, sobretudo, _sob quais condições_ ele
   escala: distinguindo empiricamente escala **vertical** (mais cores por nó) de escala
   **horizontal** (mais nós), com rigor estatístico (múltiplas repetições, média e
   desvio padrão).
4. **Que o shuffle é o gargalo prático** do sistema, evidenciando em números onde e
   por que o ganho de paralelismo satura.

Os resultados numéricos que sustentam esses objetivos são apresentados e discutidos
nas seções 6 (fase local), 7 (fase distribuída AWS) e 8 (análise comparativa).

## 2. Fundamentação Teórica

Esta seção define, com nossas palavras e ancorada nos conceitos da disciplina, os
termos que estruturam o restante do relatório. As seções seguintes **referenciam
estas definições sem redefini-las**.

### 2.1. Batch vs. stream processing

No **processamento em batch (lote)**, um conjunto de dados **finito e delimitado** é
lido por inteiro, processado e o resultado é emitido ao final. É o modelo natural
quando se tem "todos os dados de ontem" e se pode esperar por uma janela de
processamento (tipicamente noturna). Sua força é a eficiência sobre grandes volumes
estáticos; sua fraqueza é a **latência**: o resultado só existe depois que o lote
inteiro terminou.

No **processamento de fluxo (stream)**, os dados são tratados como uma sequência
**potencialmente infinita e ilimitada (_unbounded_)** de eventos que chegam de forma
contínua. O processamento é **incremental**: à medida que os eventos entram, o estado
das agregações é atualizado e resultados parciais/finais são emitidos, sem esperar o
"fim" (que não existe). É o modelo adequado a **aplicações _latency-sensitive_** —
aquelas em que o valor de uma resposta decai rapidamente com o tempo (monitoramento,
detecção de anomalias, painéis operacionais em tempo real). Este trabalho está
integralmente nesse regime.

### 2.2. Tempo de evento, tempo de processamento e watermark

Em stream processing, distinguem-se dois "relógios":

- **Event time (tempo do evento):** o instante em que o evento **de fato ocorreu no
  mundo** — no nosso caso, o horário de embarque da corrida (`pickup_datetime`). É um
  atributo _dentro_ do dado.
- **Processing time (tempo de processamento):** o instante em que o sistema **recebe e
  processa** aquele evento. Depende de latências de rede, filas e escalonamento.

Os dois divergem: um evento pode ocorrer às 10h00 (event time) mas só chegar ao Spark
às 10h05 (processing time), por atraso de rede ou porque o produtor releu um arquivo
fora de ordem. Toda análise **correta por tempo** (ex.: "quantas corridas por dia")
deve usar **event time** — senão o resultado depende de quando o dado chegou, não de
quando o fato aconteceu.

Mas isso cria um problema: se eventos podem chegar **atrasados e fora de ordem**,
quando o sistema pode considerar uma janela de tempo "completa" e emiti-la? Se
esperar para sempre, nunca fecha janela nenhuma e acumula estado infinito.

O **watermark** resolve isso. É um **limite móvel de tolerância a atraso no event
time**: uma declaração do tipo "não espero mais eventos com event time anterior a
`(maior event time já visto) − (atraso tolerado)`". Formalmente, `withWatermark(coluna,
"D")` diz ao Spark que eventos mais antigos que `D` em relação ao evento mais recente
observado são considerados **atrasados demais** e podem ser descartados. O watermark
serve a dois propósitos simultâneos:

1. **Correção:** define o ponto em que uma janela temporal pode ser "fechada",
   agregada e emitida como resultado final.
2. **Controle de memória:** uma vez que o watermark ultrapassa o fim de uma janela, o
   **estado** acumulado daquela janela pode ser **liberado**, impedindo que o estado
   cresça indefinidamente num fluxo infinito.

### 2.3. Windowing (janelamento)

Agregar um fluxo infinito exige recortá-lo em **janelas** de tempo finitas. Os três
tipos clássicos:

- **Tumbling window (fixa, não sobreposta):** janelas de tamanho fixo e disjuntas —
  cada evento pertence a exatamente uma janela. Ex.: contar corridas a cada 1 dia. É a
  que usamos nas agregações do pipeline (§3).
- **Sliding window (deslizante, sobreposta):** janelas de tamanho fixo que avançam por
  um passo menor que sua largura, de modo que se sobrepõem — um evento pode cair em
  várias janelas. Ex.: média móvel dos últimos 60 min, atualizada a cada 5 min.
- **Session window (de sessão):** janelas de tamanho variável definidas por
  **inatividade** — a janela se fecha após um intervalo sem eventos. Ex.: agrupar
  ações de um usuário até ele ficar 30 min ocioso.

Neste trabalho usamos **tumbling windows de 1 dia** sobre o event time, o que casa
naturalmente com o watermark (§2.2) para emitir cada dia uma única vez quando ele se
completa.

### 2.4. Micro-batch

O Spark Structured Streaming implementa stream por um modelo de **micro-batch**: em
vez de processar evento a evento, ele acumula os eventos que chegaram desde o último
disparo e os processa como um **pequeno lote** (um _micro-batch_) a cada _trigger_.
Cada micro-batch atualiza o estado das agregações e escreve sua saída. Essa é a
unidade fundamental de execução — e, por consequência, a unidade de medição: a
**latência de micro-batch** (§2.6, e operacionalizada na §5) é o tempo que o Spark
leva para processar um desses lotes. O modelo micro-batch aproxima o stream contínuo
por uma sucessão de lotes muito frequentes, herdando a eficiência do batch com
latência baixa.

### 2.5. Pub/sub e mensageria (Kafka)

Entre o produtor de eventos e o processador, usamos um sistema de **mensageria** no
modelo **publish/subscribe (pub/sub)**: produtores **publicam** mensagens sem saber
quem as consome, e consumidores **assinam** o fluxo sem saber quem o produz. Isso
**desacopla** as duas pontas — elas podem ter ritmos, disponibilidades e escalas
diferentes. O **Apache Kafka** é o sistema pub/sub que adotamos; seus conceitos-chave:

- **Tópico:** um canal nomeado e append-only (um _commit log_ persistente) onde as
  mensagens são publicadas. Aqui: `taxi_trips_stream`.
- **Partição:** cada tópico é dividido em **partições** — sub-logs paralelos. A
  partição é a **unidade de paralelismo e de ordenação**: o Kafka garante ordem
  **apenas dentro de uma partição**, nunca entre partições. Mais partições permitem
  mais consumidores lendo em paralelo. Nosso tópico tem **12 partições**.
- **Produtor:** publica mensagens no tópico. Ao definir uma **chave (key)** por
  mensagem, o produtor escolhe a partição por _hash_ da chave (co-localizando mensagens
  de mesma chave). Aqui a chave é o `service_type`.
- **Consumidor:** lê do tópico a partir de um **offset** (posição no log); consumidores
  de um mesmo grupo dividem as partições entre si.
- **Garantias de entrega.** Há três semânticas possíveis:
  - **At-most-once:** cada mensagem é entregue no máximo uma vez — pode se perder, mas
    nunca duplica.
  - **At-least-once:** cada mensagem é entregue ao menos uma vez — nunca se perde, mas
    pode duplicar.
  - **Exactly-once:** cada mensagem é entregue exatamente uma vez — a mais forte e a
    mais cara.
  Essas garantias dependem de configuração (ex.: `acks` do produtor, que define quantas
  réplicas confirmam a escrita). Trocar garantia por vazão é uma decisão de projeto,
  detalhada na §4.

### 2.6. Shuffle e agregações _stateful_ — o gargalo do Big Data

Operações que **reagrupam dados por chave** — `groupBy`, `join`, `reduceByKey` — têm
uma consequência inescapável num sistema distribuído: os registros de uma mesma chave
podem estar espalhados por **nós diferentes**, e precisam ser **reunidos no mesmo nó**
para serem agregados. Essa **redistribuição de dados entre os nós pela rede** é o
**shuffle**.

O shuffle é caro e é o **gargalo clássico do Big Data** por vários motivos que se
somam:

- **Atravessa a rede:** ao contrário de operações locais (map, filter), o shuffle move
  dados fisicamente entre máquinas, pagando latência e largura de banda de rede.
- **Exige serialização e I/O:** dados são serializados, muitas vezes escritos em disco
  e relidos, para atravessar a fronteira dos nós.
- **É um ponto de sincronização/coordenação:** todos os nós precisam se coordenar sobre
  quem envia o quê a quem, criando um custo de coordenação que **cresce com o número de
  nós**.

As agregações do pipeline são ainda **_stateful_ (com estado)**: como operam sobre
janelas de tempo em um fluxo, o Spark mantém um **estado** acumulado por janela entre
micro-batches (o _state store_), que precisa ser persistido, versionado (via
_checkpoint_) e, quando distribuído, compartilhado. Estado + shuffle são exatamente o
que estressamos e medimos: quando o paralelismo de CPU se esgota, é o **custo de mover
e coordenar dados** que passa a dominar — e é por isso que a §8 mostra o ganho
horizontal saturando. Toda a análise experimental deste relatório gira em torno de
onde e por que esse custo aparece.

## 3. Arquitetura da Solução

### 3.1. Visão geral do pipeline

O pipeline segue um fluxo linear de dados: os arquivos Parquet dos quatro serviços são
lidos e normalizados pelo **producer**, que publica um fluxo unificado no **Kafka**
(o _firehose_ pub/sub); o **Spark Structured Streaming** consome esse fluxo, aplica
watermark, filtragem e as duas agregações _stateful_ (que provocam shuffle), e escreve
os resultados em Parquet; o subsistema de **benchmarks** mede throughput e latência ao
longo de tudo isso.

```text
   data/ (Parquet · 4 serviços · schemas heterogêneos)
      │  lê em batches (pyarrow) · normaliza p/ EVENTO CANÔNICO (JSON)
      ▼
 ┌──────────────┐   key = service_type → partição por hash
 │  producer.py  │   alta vazão: lz4 + batching + acks=1  (§2.5 garantias)
 └──────┬───────┘
        │  publish → taxi_trips_stream
        ▼
 ┌─────────────────────────────────┐   pub/sub · commit log persistente
 │  Apache Kafka (KRaft, 1 broker)  │   tópico taxi_trips_stream · 12 partições
 └──────┬──────────────────────────┘
        │  readStream (kafka:9092 · startingOffsets=earliest)
        ▼
 ┌──────────────────────────────────────────────────────┐
 │  Spark Structured Streaming  (spark/stream_job.py)     │
 │    from_json → schema canônico                         │
 │    withWatermark(pickup_datetime)      ← event time    │
 │    filtro de anomalias (duração/tarifa)                │
 │    agregações STATEFUL (→ SHUFFLE):                    │
 │      (a) picos diários      window(1 dia).count()      │
 │      (b) tempo médio/zona   groupBy(1 dia, zona).avg() │
 │                              + JOIN tabela de zonas    │
 │  Spark Master  +  N Spark Workers (escaláveis)         │
 └──────┬─────────────────────────────────┬──────────────┘
        │  writeStream (Parquet,          │
        │   append + checkpoint)          │
        ▼                                 ▼
   output/                          benchmarks/
   ├─ daily_peaks/                   throughput (reg/s) + latência (ms)
   └─ avg_duration_by_zone/          cenários A e B · 3 repetições cada
```

Toda a orquestração vive em **Docker** (Compose no host único; Swarm no cluster AWS),
o que garante reprodutibilidade e permite escalar os workers com um único comando. As
duas fases experimentais (§6 e §7) usam **o mesmo pipeline lógico**, mudando apenas o
substrato de execução.

### 3.2. O evento canônico

Como visto na §1.2, os quatro serviços têm **schemas heterogêneos**: as colunas de
event-time têm nomes diferentes (`tpep_pickup_datetime`, `lpep_pickup_datetime`,
`pickup_datetime`), o campo de tarifa varia (`fare_amount`, `base_passenger_fare`) ou
inexiste (`fhv`), e o conjunto de colunas não coincide. Isso é um problema direto para
stream processing: o Spark precisa aplicar **um único watermark** (§2.2) e **uma única
lógica de agregação** sobre o fluxo — o que exige um schema **uniforme**. Não dá para
aplicar `withWatermark("tpep_pickup_datetime")` a um fluxo em que metade dos eventos
nem tem essa coluna.

A solução é um **evento canônico**: um único schema JSON para o qual o producer
**normaliza todas as fontes** no momento da ingestão. Cada linha de qualquer um dos
quatro Parquet é mapeada para esta estrutura:

```json
{
  "service_type":     "fhvhv|yellow|green|fhv",
  "pickup_datetime":  "2025-01-01T00:00:00",   // EVENT TIME unificado (ISO-8601)
  "dropoff_datetime": "2025-01-01T00:22:24",
  "pu_location_id":   88,                        // zona de embarque
  "do_location_id":   265,                       // zona de desembarque
  "trip_distance":    4.89,                       // milhas
  "fare_amount":      36.79,                      // null p/ fhv (preservado)
  "trip_duration_s":  1344                        // dropoff − pickup, em segundos
}
```

Três pontos de design importam aqui:

- **`pickup_datetime` como event time único.** Independentemente da coluna de origem, o
  horário de embarque vira sempre `pickup_datetime` em ISO-8601 — dando ao Spark um só
  campo sobre o qual aplicar o watermark e o janelamento (§2.3).
- **`trip_duration_s` derivado.** A duração da viagem é calculada na normalização
  (`dropoff − pickup`, em segundos), padronizando uma métrica que os schemas originais
  não expõem uniformemente.
- **`fare_amount` nullable.** O `fhv` legitimamente não tem tarifa; em vez de descartar
  esse serviço, o evento canônico admite `null`, e a filtragem de anomalias (§4/§3.3) é
  escrita para **preservar** os eventos sem tarifa em vez de tratá-los como inválidos.

Assim, um fluxo heterogêneo de quatro fontes vira **um único fluxo homogêneo** sob um
watermark único — a decisão de engenharia de dados que viabiliza todo o resto.

### 3.3. Componentes e como se conectam

| Componente | Tecnologia | Papel no pipeline |
|---|---|---|
| **Producer** | `producer/producer.py` (confluent-kafka / librdkafka) | Lê os Parquet em batches, normaliza para o evento canônico e publica no Kafka a alta vazão. É o **firehose** que simula a chegada contínua de eventos. |
| **Kafka** | Apache Kafka 3.9 (modo KRaft, sem Zookeeper) | **Firehose pub/sub** (§2.5): desacopla producer e Spark, oferece um _commit log_ persistente e particionado (12 partições) que absorve rajadas e permite consumo paralelo. |
| **Spark Master** | Spark 3.5 (Structured Streaming) | **Coordena** o cluster: recebe o job (`stream_job.py`), planeja os estágios, distribui tarefas aos workers e hospeda a UI (`:8080`) e o driver. |
| **Spark Workers** | Spark 3.5 (executores) | **Executam** o trabalho: parsing, filtragem, as agregações _stateful_ e o shuffle. São **escaláveis** — o número de workers é o fator variado no Cenário B (escala horizontal). |
| **Docker** | Compose (host único) / Swarm (AWS) | **Orquestração**: sobe todos os serviços numa rede comum, fixa versões de imagem (reprodutibilidade) e permite escalar workers com um comando (`--scale` / `service scale`). |
| **Benchmarks** | `benchmarks/` (shell + Python) | Reseta o estado, escala o cluster, produz volume fixo, roda o Spark até drenar o backlog e **coleta throughput/latência** com média e desvio padrão. |

**Como se conectam.** O producer roda no **host** e fala com o Kafka pelo listener
externo; o Spark roda em **contêineres** e fala com o Kafka pelo listener interno
(`kafka:9092`) — esse duplo endereçamento é detalhado na §4. O Spark lê o fluxo via
`readStream`, processa em micro-batches (§2.4) e grava em `output/` via `writeStream`
em **append mode com checkpoint** — o que, combinado ao watermark, faz cada janela ser
emitida uma única vez (resultado final imutável, compatível com o sink Parquet) e
permite retomar após falhas. A **elasticidade** dos workers é o que torna possível o
experimento central do trabalho: variar a capacidade e medir como o pipeline —
especialmente seu shuffle (§2.6) — escala.

## 4. Implementação

Esta seção descreve **como o código realmente implementa** a arquitetura da §3. A
lógica se divide em três artefatos: o produtor de ingestão
(`producer/producer.py`), o job de stream (`spark/stream_job.py`, submetido via
`spark/spark-submit.sh`) e a infraestrutura (`docker-compose.yml`). O foco aqui é
**justificar as decisões de engenharia** — por que cada parâmetro tem o valor que
tem e qual alternativa foi descartada.

### 4.1. Ingestão — o firehose (`producer.py`)

O produtor é o componente que transforma quatro arquivos Parquet estáticos e
heterogêneos no fluxo homogêneo de eventos canônicos (§3.2) que o Kafka absorve.
Ele faz quatro coisas: **lê em lotes**, **normaliza**, **publica** e **se autolimita
para não estourar memória nem buffer**.

#### Leitura em lotes: ler o que cabe, só as colunas que importam

O dataset (≈48,8M linhas, §1.2) não cabe confortavelmente na RAM do host (§1.3), então
carregá-lo inteiro seria inviável. A função `iter_rows` usa
`pyarrow.ParquetFile.iter_batches(batch_size=50_000, ...)`: em vez de materializar o
arquivo todo, ela percorre o Parquet em **fatias de 50 000 linhas**, convertendo uma
fatia por vez para pandas e liberando-a antes da próxima. O pico de memória fica
limitado ao tamanho de **um** batch, não do arquivo — é a materialização direta, no
código, do argumento de "volume não cabe na RAM" da §1.3.

O valor **50 000** não é mágico: é grande o bastante para amortizar o custo fixo por
batch (cada `to_pandas()` e cada `sort_values` têm overhead constante) e pequeno o
bastante para que o DataFrame temporário permaneça na casa de poucos MB. Um batch de,
digamos, 5 milhões daria pouco ganho de throughput e reintroduziria pressão de memória;
um batch de 500 multiplicaria o overhead por batch sem contrapartida.

A leitura ainda é **projetada por coluna**: `iter_batches` recebe `columns=cols`, onde
`cols` é apenas o conjunto de colunas que o mapeamento daquele serviço realmente usa
(`pickup_col`, `dropoff_col`, `pu_col`, `do_col` e, quando existem, `distance_col` e
`fare_col`). Como Parquet é colunar, ler só 5–6 colunas em vez de todas reduz o I/O de
disco e a memória proporcionalmente — não pagamos para ler colunas que seriam
descartadas na normalização. O código ainda intersecta `wanted` com
`pf.schema_arrow.names`, tolerando pequenas variações de schema entre arquivos sem
quebrar.

#### Normalização para o evento canônico

Cada `service_type` tem uma entrada em `SCHEMA_MAP` que diz **qual coluna do Parquet
preenche cada campo canônico**. É aqui que a heterogeneidade descrita na §3.2 é
resolvida na prática: `yellow` mapeia `tpep_pickup_datetime → pickup_datetime`, `green`
mapeia `lpep_pickup_datetime`, `fhv`/`fhvhv` já têm `pickup_datetime`. O serviço é
inferido do nome do arquivo por `infer_service_type`, onde a ordem de teste importa —
`fhvhv` é testado **antes** de `fhv`, senão `fhvhv_tripdata_*.parquet` casaria com o
prefixo `fhv` e seria classificado errado.

A função `_to_event` produz o dict canônico e concentra três decisões de dados já
antecipadas na §3.2:

- **`trip_duration_s` derivado** (`int((dropoff - pickup).total_seconds())`), calculado
  só quando ambos os timestamps são `datetime` válidos — padroniza uma métrica que os
  schemas de origem não expõem.
- **Colunas ausentes viram `null`.** Para `fhv`, `distance_col` e `fare_col` são `None`
  no mapa; `_to_event` produz `null` em vez de falhar. `fare_amount` é
  **deliberadamente nullable** — o `fhv` legitimamente não tem tarifa, e preservá-lo
  como `null` (em vez de descartar o serviço) é o que permite que o filtro de anomalias
  da §4.2 o mantenha.
- **Higienização de tipos.** `_num` converte `NaN`/`None` em `None` (JSON não tem NaN),
  e `_iso` serializa timestamps em ISO-8601 — a forma de string que o Spark reconverte
  em `timestamp` na §4.2.

#### Envio ao Kafka: a chave define a partição

Cada evento é serializado com `json.dumps(event, separators=(",", ":"))` — os
separadores compactos removem espaços supérfluos, encolhendo o payload que trafega
pela rede. A publicação é
`producer.produce(TOPIC, key=service_type, value=payload, ...)`.

A **`key = service_type`** não é decorativa: conforme §2.5, o Kafka escolhe a partição
por **hash da chave**, então todos os eventos de um mesmo serviço caem sempre na mesma
partição. Isso **co-localiza** cada tipo e preserva a ordem relativa dos eventos
daquele serviço dentro da sua partição (o Kafka só garante ordem *dentro* de uma
partição). Com quatro chaves distintas sobre 12 partições (§4.3), a distribuição fica
naturalmente equilibrada e o consumo paralelo do Spark não fica preso a um único tipo.
Note que **não** buscamos ordem global no broker — o tratamento de eventos fora de
ordem é responsabilidade do watermark do Spark (§2.2), não do produtor.

#### Tuning de vazão — e o trade-off de garantia

O `Producer` é configurado em `build_producer` para **maximizar throughput**, trocando
deliberadamente garantia de entrega por velocidade (o trade-off da §2.5). Cada
parâmetro e seu efeito:

- **`compression.type = "lz4"`** — comprime cada batch antes de enviá-lo pela rede. O
  payload é JSON, altamente redundante e portanto muito compressível; o lz4 foi
  escolhido (em vez de gzip/zstd) por ser **muito rápido a comprimir/descomprimir**,
  gastando pouca CPU — o que importa quando o gargalo é vazão, não espaço.
- **`linger.ms = 50`** — em vez de despachar cada mensagem isolada, o produtor **espera
  até 50 ms** acumulando mensagens antes de formar um batch. Isso troca uma latência
  mínima por lote por um ganho grande de throughput (menos requisições, batches
  maiores, melhor compressão). 50 ms é curto o bastante para não afetar a percepção de
  "tempo quase-real" e longo o bastante para encher batches sob alta vazão.
- **`batch.num.messages = 100_000`** e buffers de saída generosos
  (`queue.buffering.max.messages = 2_000_000`, `queue.buffering.max.kbytes ≈ 1 GiB`) —
  dimensionam o batch e a fila local para que o produtor **não fique ocioso**
  esperando o broker: enquanto um batch voa, os próximos já se enfileiram. Os valores
  são altos de propósito, porque o firehose empurra dezenas de milhares de msgs/s.
- **`acks = "1"`** — o produtor considera a mensagem entregue assim que o **líder** da
  partição confirma a escrita, sem esperar réplicas. Este é **o** trade-off central: com
  `acks=all` teríamos durabilidade mais forte (at-least-once robusto), mas cada envio
  esperaria a confirmação de todas as réplicas, derrubando a vazão. Como o experimento
  (i) roda com **um único broker** — onde `acks=all` e `acks=1` são praticamente
  equivalentes em durabilidade — e (ii) tolera perda/duplicação eventual de eventos
  (as agregações são estatísticas, não transacionais), `acks=1` é a escolha correta:
  entrega **at-most/at-least-once** com vazão máxima.

O produtor ainda trata **backpressure local**: se a fila interna enche, `produce`
levanta `BufferError`; o código então chama `producer.poll(0.1)` para drenar callbacks
e **tenta de novo**, em vez de descartar a mensagem. O `producer.poll(0)` após cada
envio serve callbacks pendentes sem bloquear, e `_finish` faz `flush(30)` para garantir
que a fila esvazie antes de encerrar.

#### A flag `--interleave`

Por padrão o produtor é **sequencial**: esgota o arquivo de `yellow`, depois `green`,
etc. Isso maximiza a vazão por arquivo (leitura linear do Parquet), mas tem um efeito
colateral em testes limitados: um smoke test com `--max-records 100000` consumiria só o
**primeiro** serviço, nunca amostrando os outros três. A flag `--interleave` ativa
`_round_robin`, que alterna uma linha de cada arquivo por vez (removendo do rodízio os
que se esgotam). Assim um recorte pequeno amostra **todos** os `service_type`,
exercitando o mapeamento canônico completo e o particionamento por chave — útil para
validação, embora o benchmark de vazão máxima use o modo sequencial.

### 4.2. Processamento (`stream_job.py`)

O job de stream consome o tópico, limpa, calcula as duas agregações *stateful* e grava
em Parquet. Ele é submetido por `spark-submit.sh`, que anexa o conector
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4` via `--packages` (o suporte a Kafka
não vem embutido no Spark, e a versão do pacote **precisa casar** Spark 3.5.4 + Scala
2.12 da imagem).

#### Leitura e parse

`read_kafka_stream` abre o `readStream` sobre `kafka:9092` (o listener interno, §4.3),
com `startingOffsets=earliest` para **processar todo o backlog** que o produtor já
depositou, e `failOnDataLoss=false` para tolerar truncamento/retenção do tópico durante
os smoke tests. O Kafka entrega a coluna `value` como bytes; o código faz
`from_json(value.cast("string"), CANONICAL_SCHEMA)` para reconstruir as 8 colunas do
evento canônico — o `CANONICAL_SCHEMA` do Spark **espelha exatamente** os
`CANONICAL_FIELDS` do produtor, fechando o contrato entre as duas pontas. Os campos de
data chegam como string ISO-8601 e são reconvertidos em `timestamp` com
`to_timestamp`, para que o event time volte a ser um tipo temporal comparável.

#### Watermark e filtro de anomalias

A função `clean` aplica, nesta ordem: descarte de eventos **sem** `pickup_datetime`
(sem event time não há como janelá-los), o `withWatermark("pickup_datetime", "2 hours")`
(o watermark do §2.2 sobre o event time canônico) e dois filtros de anomalia:

1. **`trip_duration_s > 0`** — remove viagens de duração não-positiva (dropoff antes ou
   igual ao pickup), que são claramente registros corrompidos e distorceriam a média de
   duração da §4.2(b).
2. **`fare_amount IS NULL OR fare_amount > 0`** — remove tarifas não-positivas
   (inválidas), mas o `IS NULL` **preserva** os eventos de `fhv`, que legitimamente não
   têm tarifa (§3.2/§4.1). Sem esse ramo, um filtro ingênuo `fare_amount > 0`
   descartaria silenciosamente todo o serviço `fhv`.

O atraso do watermark é **"2 hours"** por padrão: é a folga que damos para eventos
atrasados/fora de ordem antes de considerá-los tarde demais. Duas horas é conservador o
bastante para absorver a desordem introduzida pela releitura de arquivos e pela
distribuição por partições, sem inflar demais o estado retido. Para smoke tests curtos,
`spark-submit.sh` pode passar `--watermark-delay "0 seconds"`, forçando as janelas a
fecharem imediatamente para que o Parquet materialize dentro do tempo do teste.

#### As duas agregações com shuffle

Ambas as agregações reagrupam por chave e, portanto, **forçam shuffle** (§2.6) — é
exatamente esse custo que o trabalho estressa e mede.

**(a) Picos diários — `agg_daily_peaks`.** Faz
`groupBy(window("pickup_datetime", "1 day")).agg(count(...))`, ou seja, uma **tumbling
window de 1 dia** (§2.3) sobre o event time, contando corridas por dia. O `groupBy` por
janela força o Spark a **reunir num mesmo nó** todos os eventos de um mesmo dia, que
estão espalhados pelas partições/executores — daí o shuffle. Calcula "quantas corridas
por dia", a métrica de demanda da §1.1.

**(b) Tempo médio por zona — `agg_avg_duration_by_zone`.** Faz `groupBy(window de 1
dia, pu_location_id).agg(avg(trip_duration_s), count(...))` e depois um **join** com a
tabela de zonas. O `groupBy` por `(janela, zona)` provoca shuffle pela mesma razão —
reunir por chave composta. A janela é incluída na chave **de propósito**: um `groupBy`
só por chave não-temporal não é suportado em append mode; incluir a janela dá ao
watermark um critério de finalização (a janela fecha, o grupo é emitido). Calcula o
tempo médio de viagem por zona de embarque, a segunda métrica operacional da §1.1.

O **join com as zonas** merece nota. `load_zone_lookup` lê o
`taxi_zone_lookup.csv` (~265 linhas) **no driver via `csv`/Python puro** e materializa
o DataFrame a partir das linhas em memória, envolvido em `F.broadcast(...)`. São duas
decisões:

- **Broadcast join, não shuffle join.** A tabela de zonas é minúscula; fazer
  `broadcast` a distribui inteira a cada executor e o join vira local — **evita** um
  segundo shuffle que um join normal exigiria. Aqui o join é barato de propósito; o
  shuffle caro é o do `groupBy`.
- **Ler no driver, não via `spark.read.csv`.** No cluster multi-host (Swarm, §7) só um
  nó tem o bind-mount `/zones`. Se usássemos `spark.read.csv`, a leitura viraria uma
  *task* agendada em um executor que **não** tem o arquivo, falhando com "File does not
  exist". Ler no driver e transmitir por broadcast é o caminho correto e, para 265
  linhas, trivialmente barato.

O `spark.sql.shuffle.partitions` é fixado em **12** (tanto em `build_spark` quanto via
`spark-submit.sh`), **não** nos 200 do default. O default 200 é exagero para esta
carga e criaria centenas de tarefas de shuffle minúsculas, dominadas por overhead de
agendamento. 12 **alinha** o paralelismo de shuffle às 12 partições do tópico e ao
total de cores testado — cada tarefa de shuffle recebe trabalho suficiente para
amortizar seu custo fixo. (O valor é parametrizável por `SPARK_SHUFFLE_PARTITIONS` para
os cenários da §5.)

#### Escrita: Parquet, append, checkpoint

`start_parquet_query` grava cada agregação com `format("parquet")`,
`outputMode("append")`, um `path` de saída e um `checkpointLocation` próprios, com
`trigger(processingTime="10 seconds")` por padrão. A escolha de **append + watermark**
é o casamento explicado na §3.3: em agregação com watermark, o append emite **cada
janela uma única vez**, quando o watermark ultrapassa o fim dela — um resultado final e
imutável, que é exatamente o que um sink append-only como o Parquet exige (não dá para
reescrever uma janela já gravada). O trade-off é latência: a saída só aparece **depois**
que o watermark fecha a janela. O **checkpoint** persiste offsets do Kafka e o
*state store* das agregações, permitindo retomar exatamente de onde parou após uma
falha (§2.6).

#### Por que duas StreamingQueries separadas

As agregações (a) e (b) são duas `StreamingQuery` independentes (`q_daily` e `q_zone`),
cada uma com sua saída e seu checkpoint. A razão é uma restrição do Structured
Streaming: **não se pode encadear múltiplas agregações com watermark numa única query
em append mode**. Cada agregação *stateful* precisa ser sua própria query. As duas,
porém, partem do **mesmo `readStream`** (`cleaned`) — o Spark reaproveita a única fonte
Kafka, então não há leitura duplicada do tópico; apenas os estágios de agregação/escrita
são distintos.

Por fim, o `ProgressFileListener` grava um JSONL com um `StreamingQueryProgress` por
micro-batch (throughput e `triggerExecutionMs`), consumido pela metodologia da §5; ele
mantém contadores em memória pura para que o loop principal detecte "backlog drenado"
**sem** chamar py4j na thread principal — o que evitava um deadlock do gateway
single-thread. Os detalhes de coleta pertencem à §5.

### 4.3. Infraestrutura (`docker-compose.yml`)

Toda a topologia — Kafka, Spark master e workers — vive em contêineres orquestrados por
Compose (no host único; Swarm na AWS, §7), numa rede bridge comum `pipeline-net`.

#### Kafka em modo KRaft (sem Zookeeper)

O broker roda `apache/kafka:3.9.0` em **modo KRaft**, com `KAFKA_PROCESS_ROLES =
"broker,controller"` — um único nó acumula os dois papéis. KRaft **elimina o
Zookeeper**, que versões antigas do Kafka exigiam como serviço externo para
coordenação: menos um contêiner, menos uma dependência, setup mais simples e
reprodutível — ganho puro para um experimento de um broker. O `CLUSTER_ID` é fixado
literalmente para ser determinístico e permitir reusar o volume `kafka-data` entre
`down`/`up`.

**Os dois listeners** são o ponto sutil. O Kafka anuncia **dois endereços** para o
mesmo broker:

- **`INTERNAL://kafka:9092`** — usado pelos contêineres (o Spark). Dentro da rede
  `pipeline-net`, o DNS do Docker resolve `kafka` para o IP do contêiner.
- **`EXTERNAL://localhost:29092`** — usado pelo **produtor, que roda no host**, fora da
  rede Docker. O host não resolve o hostname `kafka`, então precisa de um endereço que
  ele entenda (`localhost`) numa porta publicada (`29092:29092`).

Dois endereços existem porque **quem se conecta de dentro e quem se conecta de fora
enxergam o broker por caminhos de rede diferentes**, e o Kafka precisa anunciar a cada
cliente o endereço **que aquele cliente consegue alcançar**. É por isso que o
`producer.py` usa `localhost:29092` por padrão e o `stream_job.py` usa `kafka:9092`. Há
ainda um terceiro listener, `CONTROLLER://kafka:9093`, interno ao quórum KRaft; ele faz
bind no hostname `kafka` (não em `0.0.0.0`) porque, em modo combinado
broker+controller, o StorageTool aborta se o endpoint do controller for `0.0.0.0`.

Um contêiner efêmero `kafka-init` cria o tópico `taxi_trips_stream` com **12
partições** e fator de replicação 1 (único broker) de forma **determinística** — mesmo
com auto-create ligado, fixar as partições explicitamente garante que o paralelismo do
tópico seja sempre o mesmo entre execuções. A escolha de **12 partições** é o teto de
paralelismo de consumo do lado do Kafka: alinha-se ao `shuffle.partitions=12` do Spark
(§4.2) e ao total de cores testado nos cenários, de modo que o gargalo estudado seja o
shuffle e o cluster, não uma subdivisão artificial do tópico.

#### Spark master + workers escaláveis

O `spark-master` tem `container_name` fixo, `hostname` estável e publica as portas 7077
(cluster) e 8080 (UI). Já o `spark-worker` **deliberadamente não tem `container_name`**:
é isso que permite `docker compose up -d --scale spark-worker=N` subir **N** réplicas —
o Compose gera nomes distintos automaticamente. Fixar um `container_name` faria o
`--scale` falhar por nome duplicado. Essa elasticidade é o mecanismo central do
experimento: variar o número de workers (escala horizontal) é literalmente reescalar
esse serviço. Cada worker recebe `SPARK_WORKER_CORES` (default 4) e
`SPARK_WORKER_MEMORY` (default 4G), ambos parametrizáveis para os cenários da §5.

O healthcheck do master sonda a UI 8080 via `/dev/tcp` do bash — a imagem
`bitnamilegacy/spark` não traz `curl`/`wget`/`nc`, então usa-se o recurso nativo do
bash. Os workers `depends_on` o master estar *healthy* antes de subir.

#### Volumes e rede

Todos os contêineres Spark montam o mesmo conjunto de volumes: `./data:/data:ro`
(Parquet de entrada, read-only), `./spark:/app:ro` (o código), `./zones:/zones:ro` (o
CSV de zonas), `./output:/output:rw` (resultados) e `./checkpoints:/checkpoints:rw` (os
checkpoints do streaming). Os read-only protegem entrada e código de escrita acidental;
os read-write recebem as saídas do job. O Kafka persiste seu log em um volume nomeado
`kafka-data`. Tudo compartilha a rede `pipeline-net`, onde o DNS do Docker viabiliza o
endereçamento por hostname que o listener interno depende.

## 5. Metodologia de Avaliação

Esta seção define **exatamente o que medimos e como** — antes de qualquer resultado.
Para que nenhum número apareça sem lastro, cada métrica das seções 6–8 tem
aqui a sua **definição operacional**: a fórmula, o campo bruto de onde vem e — igualmente
importante — **o que ela não inclui**. Os resultados propriamente ditos ficam nas seções
6 (fase local), 7 (fase AWS) e 8 (comparação); aqui descrevemos apenas o **método**.

### 5.1. O que é medido (definições operacionais)

Todas as métricas derivam de uma única fonte: o objeto **`StreamingQueryProgress`** que
o Spark emite **ao final de cada micro-batch** (§2.4). Dele extraímos dois campos brutos,
e só dois: `numInputRows` (quantas linhas de entrada aquele micro-batch consumiu do
Kafka) e `durationMs.triggerExecution` (quantos milissegundos o Spark levou para executar
aquele trigger de ponta a ponta). Toda a §5.1 é construída sobre esses dois números.

Antes das fórmulas, uma observação estrutural que atravessa tudo: o job roda **duas
`StreamingQuery` independentes** (`daily_peaks` e `avg_duration_by_zone`, §4.2), e o
listener registra **um `StreamingQueryProgress` por micro-batch de cada query**. Ambas
consomem o mesmo volume de entrada. Portanto, quando somamos `numInputRows` e
`triggerExecution` sobre **todos** os registros do run (como o parser faz), estamos
somando o trabalho das **duas** agregações — o throughput e a latência abaixo medem o
**pipeline completo** (as duas agregações com shuffle rodando em paralelo), não uma
agregação isolada. Isso é uma escolha deliberada e coerente: o que comparamos entre
configurações é a capacidade do cluster de sustentar **o pipeline inteiro**.

#### THROUGHPUT (registros por segundo)

**Definição.** Total de linhas de entrada processadas no run, dividido pela soma do
tempo de execução **apenas dos micro-batches que tiveram entrada**:

```
                        Σ numInputRows            (sobre micro-batches com numInputRows > 0)
throughput (reg/s) =  ─────────────────────────── × 1000
                        Σ triggerExecutionMs       (sobre os mesmos micro-batches)
```

Em código isto é `sum(numInputRows) / (sum_ms / 1000.0)` em
[`parse_metrics.py`](benchmarks/parse_metrics.py#L96), onde `sum_ms` acumula
`triggerExecutionMs` só dos batches não-vazios.

**De onde vem cada termo.** `numInputRows` e `durationMs.triggerExecution` são campos
crus do `StreamingQueryProgress`, capturados batch a batch pelo `ProgressFileListener`
em [`stream_job.py`](spark/stream_job.py#L118-L129) e gravados no JSONL. O parser apenas
soma e divide.

**O que este número mede.** É a **vazão de regime** do pipeline: quantas linhas por
segundo o Spark consegue efetivamente processar quando está de fato trabalhando. É a
métrica primária de escalabilidade — é ela que esperamos crescer ao adicionar cores
(Cenário A) ou workers (Cenário B).

**O que este número NÃO inclui:**

- **Não é a taxa do produtor.** O producer publica no Kafka a dezenas de milhares de
  msgs/s (§4.1); esse ritmo de ingestão **não entra** na fórmula. O throughput aqui é
  puramente a taxa de **consumo/processamento** do Spark, medida do lado do Spark.
- **Não conta o tempo ocioso.** Micro-batches vazios (`numInputRows = 0`) — os triggers
  que o Spark dispara enquanto espera dados ou depois de drenar o backlog — são
  **excluídos do denominador**. Se contássemos o tempo desses batches vazios, a vazão
  seria artificialmente diluída pelo tempo em que o sistema não tinha o que fazer.
- **Não é vazão sustentada de fonte infinita.** Cada run processa um **volume finito e
  fixo** de mensagens (§5.3); o número é a vazão média enquanto esse volume é drenado.

#### LATÊNCIA DE MICRO-BATCH (ms)

**Definição.** Média aritmética de `triggerExecutionMs` sobre os micro-batches
**não-vazios** do run:

```
                                Σ triggerExecutionMs        (batches com numInputRows > 0)
latência de micro-batch (ms) = ──────────────────────────
                                nº de micro-batches não-vazios
```

Em código, `sum(latencies) / len(latencies)` em
[`parse_metrics.py`](benchmarks/parse_metrics.py#L97), com `latencies` populada só nos
batches não-vazios.

**O que este número mede — e o ponto central a não confundir.** `triggerExecution` é
o tempo que o **Spark** leva para executar **um micro-batch de ponta a ponta do lado do
processamento**: desde o momento em que o trigger dispara e o Spark faz o *pull* dos
offsets no Kafka, passando pelo parse do JSON, filtragem, pelas agregações com shuffle
(§2.6) e pela escrita da saída em Parquet, até o commit do batch. É, portanto, a
**latência de PROCESSAMENTO de um lote pelo Spark** — não uma latência end-to-end.

**O que esta latência explicitamente NÃO é (leia com atenção):**

- **Não é latência end-to-end** (do mundo real até o resultado). Ela **não conta a
  ingestão**: o tempo que um evento espera enquanto o producer o lê do Parquet, o
  serializa, o publica no Kafka e ele fica no *commit log* até o próximo trigger — nada
  disso está aqui. O relógio de `triggerExecution` só começa quando o Spark **começa a
  processar** o lote.
- **Não conta o tempo de fila (espera no Kafka).** Se um evento chega ao tópico logo
  depois de um trigger, ele espera até o próximo trigger para ser sequer olhado; esse
  tempo de espera **não** entra na latência de micro-batch.
- **Não conta o intervalo do trigger.** O benchmark usa `trigger(processingTime="5
  seconds")` ([`run_benchmarks.sh`](benchmarks/run_benchmarks.sh#L66)); esses 5 s são a
  cadência de disparo, **não** o tempo de processamento. Um micro-batch pode disparar a
  cada 5 s mas executar em, digamos, 800 ms — é o 800 ms que a métrica reporta.

Ou seja: quando a §6 disser "latência de micro-batch = X ms", isso significa
**"o Spark leva X ms para processar um lote"**, e nada além disso. É deliberadamente uma
métrica da camada de processamento, escolhida porque é a que responde diretamente ao
paralelismo — é onde o shuffle (§2.6) cobra seu preço quando o cluster satura.

#### Por que só micro-batches não-vazios contam

Tanto o throughput quanto a latência **descartam os micro-batches com
`numInputRows = 0`** ([`parse_metrics.py`](benchmarks/parse_metrics.py#L86-L87)). A razão
é isolar o **regime de trabalho** do ruído de aquecimento e ociosidade:

- No **início** do run há triggers antes de o backlog estar disponível, e os primeiros
  batches podem incluir custo de *cold start* da query;
- No **fim**, depois que o volume fixo drena, o Spark continua disparando triggers vazios
  (é assim que o `--idle-stop` detecta que o backlog acabou, §5.3) até o job encerrar.

Incluir esses batches vazios contaminaria as duas métricas: inflaria o denominador do
throughput com tempo sem trabalho e puxaria a latência média para baixo com execuções
triviais. Medir **apenas os batches que carregaram dados** dá a característica de regime
que se quer comparar entre configurações.

#### O tamanho do micro-batch não é fixo — e o que isso implica

Um ponto essencial para interpretar a latência: **não fixamos o tamanho do
micro-batch**. Não há `maxOffsetsPerTrigger` configurado, então a cada trigger o Spark
puxa **todo o backlog disponível** naquele instante no tópico. Como o producer despeja
o volume fixo no Kafka **antes** de o job começar a drenar (§5.3), os primeiros
micro-batches tendem a ser **grandes** (muito backlog acumulado) e os seguintes,
progressivamente menores, à medida que o backlog encolhe.

Isso tem uma consequência direta na leitura dos números: a **latência de micro-batch é
uma média sobre lotes de tamanhos diferentes**, e `triggerExecution` cresce com o número
de linhas do lote. Por isso ela deve ser lida **em conjunto** com o throughput e sempre
**dentro da mesma configuração** entre repetições — não como uma latência por evento
fixa. O throughput, por ser uma razão total-linhas / total-tempo, é robusto a essa
variação de tamanho de lote (soma tudo antes de dividir); a latência média é mais
sensível a ela, e é por isso que reportamos também o **desvio padrão** (§5.3).

### 5.2. Como as métricas são coletadas

O fluxo do dado bruto do Spark até a estatística final atravessa quatro estágios, em
duas camadas distintas:

```text
  [CAMADA DE PROCESSAMENTO — Spark/JVM, dentro do container]
  StreamingQueryProgress            ← 1 objeto por micro-batch, por query
      │  ProgressFileListener.onQueryProgress()  (stream_job.py)
      ▼
  bench_metrics.jsonl               ← 1 LINHA JSON por micro-batch (append)
      │   (/output/…, volume rw compartilhado host↔container)
 ─────┼──────────────────────────────────────────────────────────
      │  [CAMADA DE MEDIÇÃO — Python puro, no host]
      ▼
  parse_metrics.py run  ───►  runs.csv     ← 1 linha por RUN (throughput+latência)
      │
      ▼
  parse_metrics.py aggregate ─►  summary.csv ← 1 linha por CONFIGURAÇÃO (média±desvio)
```

**Estágio 1 — captura (dentro do Spark).** O `ProgressFileListener` é registrado como um
`StreamingQueryListener` ([`stream_job.py`](spark/stream_job.py#L83)). O Spark chama seu
`onQueryProgress` ao final de cada micro-batch de cada query, passando o
`StreamingQueryProgress`. Optamos por esse listener porque o `StreamingQueryProgress`
**não é exposto pelo endpoint REST** do Spark para Structured Streaming (o `/streaming`
da API REST é exclusivo do DStreams legado) — o listener é a fonte canônica da métrica.

**Estágio 2 — persistência (JSONL).** Cada progresso vira **uma linha JSON** anexada a
`bench_metrics.jsonl`, contendo os campos crus (`numInputRows`, `triggerExecutionMs`,
`batchId`, timestamps etc.). O arquivo fica em `/output`, um volume `rw` montado no host
(§4.3), de modo que o host lê exatamente o que o container escreveu. Cada run **trunca**
o arquivo no início ([`stream_job.py`](spark/stream_job.py#L324)) e o harness o **arquiva**
por run em `results/metrics/<run_id>.jsonl`
([`run_benchmarks.sh`](benchmarks/run_benchmarks.sh#L162-L176)), preservando o dado bruto
de cada execução para auditoria.

**Estágio 3 — resumo por run.** `parse_metrics.py run` lê o JSONL do run, aplica as
fórmulas da §5.1 (descartando batches vazios) e **anexa uma linha** ao `runs.csv` com o
throughput e a latência daquele run, mais os contadores de apoio (`total_input_rows`,
`n_nonempty_batches`, `sum_trigger_ms_nonempty`) e os rótulos da configuração (cenário,
parâmetro, valor, workers, repetição).

**Estágio 4 — agregação por configuração.** Ao final de todos os runs,
`parse_metrics.py aggregate` lê o `runs.csv`, **agrupa as linhas por configuração** — a
tupla `(scenario, param, value, workers)` — e para cada grupo calcula **média e desvio
padrão** de throughput e de latência, gravando `summary.csv` (uma linha por
configuração). É este arquivo que alimenta as tabelas e figuras das §6–8.

**A distinção de camadas importa.** O **cálculo estatístico não roda dentro do Spark**:
o Spark (JVM/py4j) apenas *emite* os progressos; toda a agregação (soma, média, desvio)
é feita por **Python puro no host**, sem tocar o cluster. Isso mantém a medição
**barata e não-intrusiva** — o parser não compete por recursos com o job medido — e
**auditável**: o `runs.csv` e os JSONL arquivados permitem refazer qualquer número do
relatório a partir do dado bruto. (Há inclusive um motivo de robustez para o listener
manter contadores em memória pura: o loop de encerramento do job os lê **sem** chamar
py4j, evitando o deadlock do gateway *single-thread* descrito na §4.2 — a instrumentação
foi desenhada para não perturbar o que mede.)

### 5.3. Desenho experimental

O objetivo do experimento é **isolar o efeito de adicionar capacidade computacional** de
duas formas distintas. Para isso rodamos **dois cenários**, cada um variando **um único
fator** e mantendo o resto fixo.

#### Os dois cenários

**Cenário A — escala VERTICAL (mais cores por nó).** Varia
`spark.executor.cores ∈ {1, 2, 4}` com o **número de workers fixo** (1). Cada run dá ao
executor mais cores no **mesmo nó**. O que A isola: o ganho de **paralelismo intra-nó** —
quanto mais tarefas simultâneas em uma máquina aceleram o pipeline, sem envolver rede
entre nós. É o teto de escala vertical.

**Cenário B — escala HORIZONTAL (mais nós).** Varia o número de `spark-worker ∈ {1, 2, 3}`
com os **cores por executor fixos** (4). Aqui o `total-executor-cores` escala junto
(`workers × 4`) para que o app de fato use todos os workers
([`run_benchmarks.sh`](benchmarks/run_benchmarks.sh#L207-L218)). O que B isola: o ganho de
**paralelismo entre nós** — e, crucialmente, é o cenário em que o **shuffle** (§2.6)
passa a atravessar a rede entre máquinas. É onde esperamos ver o ganho horizontal
**saturar** quando o custo de mover e coordenar dados domina (analisado na §8).

A separação em dois cenários de fator único é o que permite atribuir causa: se
variássemos cores e workers ao mesmo tempo, não saberíamos a qual dos dois um ganho (ou
uma saturação) deveria ser creditado. A e B são justamente as duas perguntas
"mais cores por nó ajuda?" e "mais nós ajuda?" respondidas **separadamente**.

#### Por que 3 repetições — média e desvio padrão

Cada configuração é executada **3 vezes** (`REPS=3`), e reportamos a **média** das três
como o valor central e o **desvio padrão amostral** como medida de dispersão. A medição
de um sistema real tem ruído — escalonamento do SO, variação de GC da JVM, estado de
cache, jitter de rede — e uma única execução pode calhar de ser atípica. Repetir e
promediar reduz o peso do run atípico; o desvio padrão **quantifica quanta variação
sobra** entre repetições da *mesma* configuração.

Usamos o **desvio padrão amostral**, com denominador **n − 1** (correção de Bessel), e
não o populacional (n) — ver
[`parse_metrics.py`](benchmarks/parse_metrics.py#L110-L119):

```
        ┌───────────────────────────
        │   Σ (vᵢ − média)²
  s  =  │  ─────────────────────
      ╲ │        n − 1
       ╲│
```

Dividimos por `n − 1` porque as 3 execuções são uma **amostra** de todas as execuções
possíveis daquela configuração, não a população inteira; a média usada no cálculo é
estimada **da própria amostra**, o que "consome" um grau de liberdade e faz o divisor n
subestimar a variância real. O n − 1 corrige esse viés (estimador não-enviesado da
variância). Com `n = 1` o desvio é definido como 0 (não há dispersão a estimar). Uma
observação honesta de rigor: com apenas 3 pontos o desvio padrão é ele mesmo uma
estimativa grosseira — não o tratamos como um intervalo de confiança formal, e sim como
um **indicador de confiabilidade**: um desvio pequeno em relação à média indica que a
diferença entre configurações é real e não artefato de ruído; um desvio grande pede
cautela ao comparar médias próximas. É assim que ele deve ser lido nas §6–8.

#### Controle de variáveis (reprodutibilidade)

Para que a comparação entre configurações seja legítima, tudo que **não** é o fator sob
teste é mantido constante a cada run
([`run_benchmarks.sh`](benchmarks/run_benchmarks.sh#L91-L105), `reset_state` e
`one_run`):

- **Volume fixo por run.** Todo run processa o **mesmo** número de mensagens
  (`MAX_RECORDS`, injetado pelo producer com `--max-records` antes de o job drenar). O
  volume é uma constante do experimento, não uma variável — logo, diferenças de
  throughput/latência refletem a **configuração**, não quanto dado cada run viu.
- **Estado zerado a cada run.** Antes de cada execução, o harness **recria o tópico**
  Kafka (delete + create com as 12 partições) e apaga `output/` e `checkpoints/`. Isso
  garante que cada run parta do **mesmo estado limpo**: sem o reset, o
  `startingOffsets=earliest` combinado ao checkpoint reaproveitaria offsets e estado de
  runs anteriores, e os runs não seriam comparáveis nem reproduzíveis.
- **Fatores fixos por cenário.** No Cenário A, workers = 1 em todos os runs; no B, cores
  por executor = 4 em todos. O `trigger` (5 s), o watermark do benchmark
  (`0 seconds`, para as janelas fecharem dentro do teste, §4.2), o
  `shuffle.partitions = 12` e o número de partições do tópico (12) são constantes em
  ambos os cenários.

O resultado é um protocolo **reproduzível**: mesmo volume, mesmo estado inicial, mesmo
pipeline lógico, variando um único fator por vez e repetindo três vezes — a base sobre a
qual as seções 6 (local) e 7 (AWS) reportam seus números e a seção 8 os compara.

## 6. Fase 1 — Experimento Local (host único)

Esta seção reporta a **primeira** das duas fases experimentais: rodar o pipeline
inteiro — Kafka, Spark master e workers — num **único host físico**, sob o protocolo
da §5 (dois cenários de fator único, volume fixo, estado zerado, 3 repetições, média ±
desvio padrão). O objetivo é responder empiricamente às duas perguntas da §5.3
**dentro de uma só máquina**: adicionar cores por executor (escala vertical) acelera o
pipeline? E adicionar workers (escala horizontal) — que aqui ainda é uma escala
"horizontal simulada", pois todos os workers compartilham o mesmo hardware — acelera?
A resposta a essa segunda pergunta é o que motiva a Fase 2 na AWS (§7).

Todas as métricas abaixo seguem as **definições operacionais da §5.1** e não são
redefinidas: **throughput** = `Σ numInputRows / Σ triggerExecutionMs × 1000` sobre os
micro-batches não-vazios (vazão de **processamento** do Spark, não a taxa do producer);
**latência de micro-batch** = média de `triggerExecutionMs` dos batches não-vazios
(tempo de **processamento** de um lote pelo Spark, **não** end-to-end). Ambas medem o
**pipeline completo** — as duas agregações _stateful_ (`daily_peaks` e
`avg_duration_by_zone`) somadas, §5.1.

### 6.1. Ambiente de teste

O experimento local roda sobre um **host único** com as seguintes características:

| Recurso | Valor |
|---|---|
| CPU | 12 núcleos lógicos |
| RAM | 16 GB |
| Orquestração | Docker 28 / Compose v2 (todos os serviços em contêineres numa rede _bridge_ comum) |
| SO | Ubuntu Linux |
| Producer | roda no **host** (dentro de `venv`), fala com o Kafka pelo listener EXTERNAL (`localhost:29092`, §4.3) |

O **subconjunto de dados** é o descrito na §1.2: **≈ 48,8 milhões de linhas / 1,07 GB**
em Parquet, cobrindo **dois meses** e os **quatro serviços** (`yellow`, `green`, `fhv`,
`fhvhv`), normalizados para o evento canônico (§3.2). Esse é o corpus de onde o producer
extrai as mensagens em cada run.

**Volume por run e repetições.** Cada run injeta um volume **fixo** de mensagens antes
de o job começar a drenar (§5.3). Nos números finais, cada run produz **2.000.000 de
mensagens**, que o Spark processa nas **duas** queries — daí `total_input_rows =
2.000.000 × 2 = 4.000.000` de linhas de entrada agregadas por run (visível na coluna
`total_input_rows` de `runs_FINAL.csv`). Cada configuração é repetida **3 vezes**
(`REPS=3`); reportamos a média e o desvio padrão amostral (n − 1, §5.3). Entre runs, o
harness recria o tópico com 12 partições e limpa `output/`/`checkpoints/`, garantindo
que cada execução parta do mesmo estado limpo. Um detalhe da leitura: o volume fixo
drena em **2 micro-batches não-vazios** por run (coluna `n_nonempty_batches = 2` em
todos os runs) — coerente com o modelo de micro-batch de tamanho variável da §5.1, em
que o primeiro trigger puxa quase todo o backlog acumulado.

### 6.2. Resultados — Cenário A (escala vertical, variar cores)

O Cenário A varia `spark.executor.cores ∈ {1, 2, 4}` com **1 worker fixo** (§5.3),
isolando o ganho de **paralelismo intra-nó**.

| cores | throughput (reg/s) | latência micro-batch (ms) |
|---:|---:|---:|
| 1 | 23.424,5 ± 471,4 | 42.701,8 ± 857,1 |
| 2 | 32.544,6 ± 472,9 | 30.731,3 ± 443,2 |
| 4 | 46.333,6 ± 80,6 | 21.582,7 ± 37,5 |

_(painéis esquerdos de [`painel_completo.png`](benchmarks/results/figs/painel_completo.png);
também em [`A_throughput.png`](benchmarks/results/figs/A_throughput.png) e
[`A_latency.png`](benchmarks/results/figs/A_latency.png).)_

**O que esses números significam.** À luz das definições da §5.1, o throughput é a
vazão de **processamento** do Spark, e ela **cresce de forma quase-linear** com os
cores: de 1 → 2 cores o ganho é **1,39×** (23.424 → 32.545), de 2 → 4 é **1,42×**
(32.545 → 46.334) e, de ponta a ponta, dobrar-e-dobrar os cores (1 → 4, um fator 4×)
dá **1,98×** de throughput. Não é linearidade perfeita (4× de cores não dá 4× de
vazão), e nem se esperaria: as agregações do pipeline fazem **shuffle** (§2.6), que tem
uma fração inerentemente serial — coordenação, escrita/leitura de estado, o join por
broadcast — que não paraleliza com os cores (é o **limite de Amdahl** clássico). Ainda
assim, um fator ≈2× ao quadruplicar os cores é um retorno **forte e consistente**: para
esta carga, dar mais cores ao executor é dinheiro bem gasto. O paralelismo intra-nó
está de fato sendo convertido em trabalho útil.

**A latência cai na direção oposta — e coerentemente.** A latência de micro-batch
**diminui** de 42.702 ms (1 core) para 21.583 ms (4 cores), quase **pela metade**. É o
espelho esperado do throughput: como o volume por run é **fixo** (§5.3) e o tamanho do
micro-batch não é limitado (§5.1), cada trigger processa aproximadamente o mesmo número
de linhas independentemente dos cores; com mais cores, o Spark processa esse mesmo lote
**mais rápido**, então `triggerExecutionMs` cai. Vale reafirmar a honestidade da §5.1:
esses ~21–43 **segundos** por micro-batch **não** são "latência em tempo real" — são o
tempo de **processamento de um lote grande** (≈2M linhas por batch), inflado justamente
porque não fixamos `maxOffsetsPerTrigger` e o primeiro trigger engole quase todo o
backlog. A métrica é comparável **entre configurações** (o que interessa aqui), mas não
deve ser lida como o atraso percebido por um evento individual.

**O desvio padrão diminui drasticamente com mais cores** — e isso não é um detalhe. Em
termos **relativos à média**, o desvio do throughput cai de **2,0 %** (471,4 / 23.424)
com 1 core para **0,17 %** (80,6 / 46.334) com 4 cores; na latência, de **2,0 %** para
**0,17 %** também. O que isso indica: com poucos cores, o executor **disputa** os
recursos do host com o resto do sistema (o próprio producer, o broker Kafka, o SO), e
essa contenção introduz **jitter** entre repetições — cada run calha de pegar uma fatia
de CPU ligeiramente diferente. Com 4 cores dedicados ao trabalho, a execução fica
**previsível e estável**, e as três repetições praticamente coincidem. Pela leitura do
desvio que a §5.3 prescreve (desvio pequeno ⇒ diferença real, não ruído), a escada de
throughput do Cenário A é **inequívoca**: as barras não se sobrepõem dentro de suas
margens de erro, então o ganho vertical é um efeito real do hardware, não artefato de
medição.

### 6.3. Resultados — Cenário B (escala horizontal, variar workers)

O Cenário B varia o número de `spark-worker ∈ {1, 2, 3}` com **4 cores por executor
fixos** (§5.3) e `total-executor-cores = workers × 4`, isolando o ganho de
**paralelismo entre nós** (aqui, entre contêineres no **mesmo host**).

| workers | cores totais | throughput (reg/s) | latência micro-batch (ms) |
|---:|---:|---:|---:|
| 1 | 4 | 46.084,2 ± 1.349,1 | 21.712,0 ± 645,5 |
| 2 | 8 | 46.508,3 ± 523,5 | 21.503,3 ± 241,4 |
| 3 | 12 | 42.648,4 ± 182,4 | 23.447,8 ± 100,0 |

_(painéis direitos de [`painel_completo.png`](benchmarks/results/figs/painel_completo.png);
também em [`B_throughput.png`](benchmarks/results/figs/B_throughput.png) e
[`B_latency.png`](benchmarks/results/figs/B_latency.png).)_

**O que esses números significam — e por que estagnaram.** Diferentemente do Cenário A,
adicionar workers **não** aumenta o throughput: de 1 para 2 workers ele fica
praticamente **plano** (46.084 → 46.508, um ganho de apenas **0,9 %**, dentro do desvio
padrão — ou seja, estatisticamente indistinguível), e de 2 para 3 workers ele
**piora** (46.508 → 42.648, uma **queda de 8,3 %**). A latência acompanha o espelho:
estável de 1 → 2 workers (~21,5 s) e **subindo** para 23,4 s com 3 workers. Note ainda
que **dobrar os cores totais de 4 para 8** (1 → 2 workers) produziu ganho nulo, ao passo
que no Cenário A dobrar cores (1 → 2) rendeu 1,39×. **O mesmo recurso — cores — que era
valioso no A é inútil no B.** A diferença não está no número de cores, mas em **como**
eles são adicionados: no A, mais cores no **mesmo** executor; no B, mais cores em
executores **separados**, que precisam se coordenar.

**A explicação técnica.** O host tem **12 cores**. Com 3 workers × 4 cores cada = **12
cores**, o cluster requisita **exatamente toda a CPU física da máquina** — e, na
prática, mais do que ela, porque o producer, o broker Kafka, o Spark master/driver e o
próprio SO **também** precisam de CPU no mesmo host. O resultado é **saturação e
contenção**: os workers passam a disputar os mesmos núcleos, e o tempo que ganhariam
processando em paralelo é perdido em troca de contexto e espera por CPU. Some-se a isso
o custo estrutural que a §2.6 antecipa: ao distribuir o trabalho por **mais executores
separados**, o **shuffle** das duas agregações passa a **cruzar a fronteira dos
executores** — dados de uma mesma janela/zona que caíram em workers diferentes precisam
ser reunidos, agora pagando serialização e coordenação **entre** processos, não dentro
de um só. Em um único host isso não paga latência de rede (é _loopback_), mas paga
**tudo o mais**: serialização, I/O de shuffle, e o custo de sincronização que
**cresce com o número de nós**.

Em outras palavras, à medida que se sobe de 1 para 3 workers **neste host**, o
**gargalo migra de CPU para coordenação/shuffle**: com 1–2 workers ainda há CPU
sobrando para absorver a contenção e o ganho de coordenação empata com a perda; com 3
workers a CPU esgota, a coordenação passa a dominar, e o pipeline anda **para trás**.
O desvio padrão reforça a leitura: ele **encolhe** com mais workers (de 2,9 % da média
com 1 worker para 0,4 % com 3), o que significa que a **piora** com 3 workers é
**consistente e real** — não é um run azarado, é o teto do host se manifestando de
forma reprodutível.

### 6.4. Discussão da Fase Local

Os dois cenários, lidos juntos, contam uma história limpa sobre **este ambiente**:

- **A escala vertical (mais cores por nó) funciona.** O Cenário A mostra ganho
  quase-linear (1,98× de throughput ao quadruplicar os cores) e **estabiliza** a
  medição (desvio caindo para 0,17 %). Enquanto há cores ociosos no host para dar a
  **um** executor, o pipeline os converte em vazão. Esse é o resultado esperado quando
  o gargalo é **CPU** e a fração serial (shuffle) ainda é pequena diante do trabalho
  paralelizável.

- **A escala horizontal (mais workers) satura — _neste_ host.** O Cenário B mostra
  throughput plano de 1 → 2 workers e **regressão** com 3. A causa não é uma falha do
  Spark nem do desenho do pipeline: é que **3 workers × 4 cores saturam os 12 cores
  físicos** do host único, e a partir daí acrescentar "nós" só acrescenta **contenção e
  custo de coordenação/shuffle** (§2.6) sobre um hardware que já não tem folga. O
  paralelismo entre nós só compensa seu overhead quando cada nó tem **recursos
  próprios** — o que, num host único, é impossível por construção.

**A limitação central é o host único**, e ela é fundamental, não incidental. No Cenário
B, "adicionar um worker" **não adiciona uma máquina** — apenas fatia a mesma CPU em mais
processos concorrentes. Logo, este experimento **não consegue medir escala horizontal
de verdade**: a variável que ele varia (nº de workers) não vem acompanhada da variável
que a tornaria útil (mais hardware). Para testar se o pipeline **realmente** escala
horizontalmente — se o shuffle atravessando a **rede** entre máquinas distintas satura
ou não, e onde — é preciso dar a **cada worker seu próprio host**.

É exatamente isso que motiva a **Fase 2 (§7)**: levar o mesmo pipeline lógico
(§3, §4) para um **cluster de VMs na AWS**, onde cada worker roda em uma máquina com CPU
própria e o shuffle passa a pagar **latência de rede real**. Só nesse substrato a
pergunta "mais nós ajuda?" pode ser respondida sem o confundidor do host único — e a
§8 compara os dois ambientes lado a lado para isolar o que era limite do hardware local
e o que é limite intrínseco do shuffle distribuído.

Uma nota final de honestidade metodológica, coerente com o cuidado de dar lastro a cada
número que o relatório adota. Além de a latência de micro-batch **não** ser tempo real (são segundos de
processamento de lotes grandes, §5.1/§6.2), há um segundo viés que o leitor deve ter em
mente: a chave de particionamento do producer é `key = service_type` (§4.1), e há apenas
**quatro** valores distintos. Isso significa que, embora o tópico tenha 12 partições, os
eventos se concentram em **no máximo 4 partições ativas** — e como os quatro serviços
têm volumes muito díspares (`fhvhv` domina, `green` é minúsculo, §1.2), a carga entre
partições é **desbalanceada**. Esse desbalanceamento limita, por si só, o paralelismo de
consumo do Spark e é mais um fator que ajuda a explicar por que o ganho horizontal do
Cenário B não deslancha — o shuffle não tem 12 fluxos equilibrados para paralelizar, e
sim uns poucos grandes. Registramos isso abertamente: os números da Fase Local são
sólidos **para comparar configurações sob o mesmo desenho**, mas carregam essas duas
ressalvas de interpretação.

## 7. Fase 2 — Experimento Distribuído (AWS)

A Fase 1 (§6) respondeu com clareza à pergunta da escala **vertical** (mais cores por
nó ajuda — §6.2), mas deixou a pergunta da escala **horizontal** em aberto por uma
limitação estrutural do substrato: num host único, "adicionar um worker" não adiciona
uma máquina, apenas fatia a mesma CPU (§6.4). Esta seção reporta a segunda fase, que
existe precisamente para remover esse confundidor: o **mesmo pipeline lógico** (§3, §4),
sob o **mesmo protocolo** (§5), levado para um **cluster de VMs reais na AWS**, onde cada
worker roda em uma máquina com CPU própria e o shuffle passa a pagar **latência de rede
de verdade**.

Todas as métricas continuam as da §5.1 e **não são redefinidas**: **throughput** =
`Σ numInputRows / Σ triggerExecutionMs × 1000` sobre os micro-batches não-vazios (vazão
de **processamento** do Spark, não a taxa do producer); **latência de micro-batch** =
média de `triggerExecutionMs` dos batches não-vazios (tempo de **processamento** de um
lote pelo Spark, **não** end-to-end). Ambas medem o **pipeline completo** — as duas
agregações _stateful_ somadas (§5.1).

### 7.1. Motivação: por que ir para VMs

A crítica que a Fase Local não consegue responder por si é direta: **um host único não
prova escala horizontal real.** Quando o Cenário B da §6.3 sobe de 1 para 3 workers, ele
não distribui o trabalho por mais máquinas — ele multiplica processos concorrentes sobre
os **mesmos 12 cores físicos**, que ainda são disputados pelo producer, pelo broker
Kafka, pelo driver e pelo SO. O resultado (throughput plano de 1→2 workers e regressão em
3) é indistinguível de duas hipóteses muito diferentes:

1. **"O pipeline não escala horizontalmente"** — o shuffle distribuído é caro demais e o
   ganho de nós nunca compensa; ou
2. **"O host único saturou"** — o pipeline até escalaria, mas não há hardware novo para
   os workers extras ocuparem.

No host único essas duas explicações **colapsam na mesma curva** — não há como separá-las,
porque a variável que se varia (nº de workers) nunca vem acompanhada da variável que a
tornaria útil (mais CPU física). A única forma de decidir entre (1) e (2) é dar a **cada
worker seu próprio host** e observar o que acontece. Se o throughput passar a **crescer**
ao adicionar workers, a explicação era (2) — o limite era do hardware local, não do
pipeline. É esse experimento decisivo que a Fase 2 executa.

### 7.2. Arquitetura AWS

O cluster tem **4 VMs EC2** na região `us-east-1`, todas na **mesma AZ e subnet**
(`us-east-1a`, para minimizar latência de rede entre elas), rodando Ubuntu 24.04 e
orquestradas por **Docker Swarm**. A separação de papéis é deliberada:

| Papel | VM | Tipo | vCPU | Função no cluster |
|---|---|---|---:|---|
| **infra** (manager) | node-infra | t3.large | 2 | Kafka broker, Spark **master/driver**, producer (no host), NFS server |
| **worker** | node-w1 | t3.medium | 2 | 1 Spark **executor** |
| **worker** | node-w2 | t3.medium | 2 | 1 Spark **executor** |
| **worker** | node-w3 | t3.medium | 2 | 1 Spark **executor** |

**Diagrama de topologia:**

```text
                     ┌───────────────────────────────────────────────┐
                     │  node-infra  (t3.large · 172.31.8.90 · manager) │
                     │                                                 │
   producer.py ──────┼─► Kafka (KRaft, 1 broker) ── EXTERNAL           │
   (no HOST,         │      taxi_trips_stream_<run> · 12 partições     │
    localhost:29092) │   Spark MASTER + DRIVER (UI :8080)              │
                     │   NFS server → export output/ + checkpoints/    │
                     └───────┬───────────────────────────┬────────────┘
                             │  rede OVERLAY (VXLAN)      │  NFS (172.31.0.0/16)
             ┌───────────────┼───────────────┬───────────┴───────────┐
             ▼               ▼               ▼                        │
     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
     │  node-w1     │ │  node-w2     │ │  node-w3     │   (mesmo path │
     │  t3.medium   │ │  t3.medium   │ │  t3.medium   │    /output,   │
     │ spark-worker │ │ spark-worker │ │ spark-worker │    /checkpts  │
     │  (executor)  │ │  (executor)  │ │  (executor)  │    montado)   │
     │  INTERNAL──► kafka:9092 (DNS overlay → VIP)     │◄─────────────┘
     └──────────────┘ └──────────────┘ └──────────────┘
      max_replicas_per_node: 1  ⇒  1 worker por VM (escala horizontal REAL)
```

Três decisões de arquitetura sustentam o experimento:

- **Rede overlay _attachable_.** No `docker-stack.yml` a rede `pipeline-net` passa de
  `driver: bridge` (host único) para `driver: overlay`. A overlay do Swarm encapsula o
  tráfego dos contêineres em **VXLAN** sobre a rede das VMs (UDP 4789), de modo que um
  spark-worker em node-w3 enxerga `kafka` (em node-infra) por DNS interno — o nome de
  serviço resolve para o **VIP do serviço**. É o que permite ao advertised
  `INTERNAL://kafka:9092` ser alcançável **entre VMs** sem hard-code de IP.

- **Placement por label — 1 worker por VM.** Cada nó recebe um label (`role=infra` ou
  `role=worker`). Kafka, kafka-init e spark-master têm `constraints:
  node.labels.role==infra` (ficam na VM de infra); o spark-worker é `mode: replicated`
  com **`placement.max_replicas_per_node: 1`** e `role==worker`. Essa única linha é o
  coração da Fase 2: ela **garante que cada réplica de worker caia numa VM física
  distinta**, sem co-locação. Reescalar de 1→2→3 workers (`docker service scale
  taxi_spark-worker=N`) é, portanto, literalmente **adicionar máquinas** — a escala
  horizontal que o host único não conseguia oferecer.

- **Driver fixo + storage compartilhado.** O driver roda sempre no master (node-infra),
  então o código do job, o CSV de zonas e os resultados vivem só ali. Mas as agregações
  _stateful_ têm um problema que só aparece em multi-host, tratado na §7.3.

O tópico Kafka mantém as **12 partições** e a chave `service_type`, e o
`spark.sql.shuffle.partitions=12`, exatamente como na §4 — o pipeline lógico é idêntico;
muda apenas o substrato.

### 7.3. Desafios da distribuição

Esta subseção é o coração de engenharia da Fase 2. Tudo que "simplesmente funcionava" no
host único dependia, silenciosamente, de **um recurso compartilhado implícito**: um só
disco, um só sistema de arquivos, um só usuário, um só broker no mesmo `localhost`. No
multi-host cada VM tem disco próprio, usuário próprio e só se fala pela rede — e cada uma
dessas dependências implícitas quebrou. Foram **seis** problemas; para cada um, o
sintoma, **por que só aparece em multi-host**, e a correção.

#### 1. Disco EBS cheio (8 GB)

**Sintoma.** No primeiro deploy, o download dos jars do conector Kafka (via Ivy/Maven)
falhava com `No space left on device`; a raiz da t3.large estava a 100 %.

**Por que só em multi-host.** No host de desenvolvimento havia dezenas de GB livres. A
VM foi provisionada com o volume EBS **padrão de 8 GB**, que a imagem Docker do Spark
(≈2 GB) + Kafka + os jars já esgotam. É um limite que simplesmente não existia na máquina
local.

**Correção.** Cresci o volume EBS `vol-084633dbc596a1c7b` para 20 GB (`aws ec2
modify-volume`), depois `growpart /dev/nvme0n1 1` + `resize2fs` para o SO enxergar o
espaço novo. (Detalhe: o `growpart` precisa de espaço em `/tmp` — foi preciso `docker
prune` antes.)

#### 2. Cache Ivy perdido a cada run (UID do container)

**Sintoma.** A cada run, os 11 jars do conector Kafka eram **rebaixados de novo** do
Maven Central, somando minutos e, sob rede instável, travando o run.

**Por que só em multi-host.** O `spark-submit` resolve os `--packages` via Ivy, que por
padrão cacheia em `~/.ivy2`. A imagem `bitnamilegacy/spark` roda como **UID 1001 com
`HOME=/`**, e `/` não é gravável pelo executor — então o cache **nunca persistia**. No
host único isso passava despercebido porque o container era efêmero e reusado; no cluster,
com resets frequentes entre 15 runs, o custo repetido ficou proibitivo.

**Correção.** Bind-mount dedicado `/home/ubuntu/taxi/ivy:/ivy` + `--conf
spark.jars.ivy=/ivy` no `spark-submit.sh`, apontando o cache para um diretório **gravável
e persistente**; pré-aquecido uma vez (os 11 jars baixados) e reusado em todos os runs
seguintes.

#### 3. Permission denied em output/ e checkpoints/

**Sintoma.** O job falhava ao escrever o JSONL de métricas e o Parquet de saída:
`Permission denied`.

**Por que só em multi-host.** Os diretórios foram criados pelo usuário `ubuntu`
(UID 1000) via `distribute_files.sh`, mas o processo Spark dentro do container escreve
como **UID 1001**. No host de desenvolvimento os diretórios já pertenciam ao usuário que
subia o Compose; na VM, o descasamento de UID entre quem criou e quem escreve virou um
erro de permissão imediato.

**Correção.** `chmod 777 output/ checkpoints/` — abrir a escrita para qualquer UID.
Aceitável num cluster efêmero de benchmark; não seria num ambiente de produção.

#### 4. Tabela de zonas ausente nos executores (→ broadcast)

**Sintoma.** A agregação (b) (tempo médio por zona, §4.2) falhava com `File does not
exist: /zones/taxi_zone_lookup.csv`, mesmo com o arquivo presente no node-infra.

**Por que só em multi-host.** O join precisa da tabela de zonas. Se ela for lida com
`spark.read.csv("/zones/...")`, essa leitura vira uma **task agendada em um executor** —
e o executor está numa **VM worker que não tem o bind-mount `/zones`** (só o infra o tem,
§7.2). No host único o executor e o arquivo estavam na **mesma** máquina, então a leitura
sempre encontrava o arquivo; a distribuição expôs que a task podia cair em qualquer nó.

**Correção.** `load_zone_lookup()` passou a ler o CSV (~265 linhas) **no driver**, em
Python puro, e materializar o DataFrame via `F.broadcast(spark.createDataFrame(...))`. O
driver está sempre no infra (onde o arquivo existe), e o broadcast transmite a tabela
inteira a cada executor — transformando o join num **broadcast join local** (§4.2), que é
o correto para uma tabela minúscula e ainda **evita um segundo shuffle**.

#### 5. State store escrito pelos executores (→ NFS entre VMs)

**Sintoma.** Mesmo com permissões corrigidas, o job falhava com `mkdir of
file:/checkpoints/... failed` nas VMs worker.

**Por que só em multi-host — e por que é o problema mais sutil.** Este é o cerne do
Structured Streaming distribuído. O **state store** (o estado _stateful_ das agregações,
§2.6) e o output Parquet **não** são escritos só pelo driver: são escritos pelos
**executores**, que rodam nas VMs worker. Como os paths são `file://` locais
(`/checkpoints`, `/output`), cada executor tentava escrever num diretório que só existia,
de fato, no node-infra. No host único **todos os processos compartilhavam o mesmo sistema
de arquivos**, então "escrever em `/checkpoints`" sempre acertava o mesmo lugar; em
multi-host, cada VM tem o seu, e o estado ficaria fragmentado (ou faltando) por nó.

**Correção.** Montei **NFS**: o node-infra **exporta** `output/` e `checkpoints/` para a
sub-rede `172.31.0.0/16`, e as 3 VMs worker montam esses exports **no mesmo path**
(`/home/ubuntu/taxi/{output,checkpoints}`, via fstab), que por sua vez são os bind-mounts
do serviço `spark-worker`. Assim todo executor vê o **mesmo** diretório de estado,
fisicamente no infra. O export usa `all_squash,anonuid=1001,anongid=1001` para casar o
UID 1001 do container (problema 3 de novo) + dirs 777. (Nota: `user: "1000:1000"` no
serviço **não** resolve, porque o entrypoint bitnami precisa escrever em
`/opt/bitnami/spark/tmp`, cujo dono é 1001.)

#### 6. Offsets _stale_ do KRaft (→ tópico único por run)

**Sintoma.** O **primeiro** run de cada sessão processava normalmente, mas todos os runs
**seguintes** liam **0 linhas**, com o log do Spark avisando `KafkaOffsetReaderAdmin:
Found incorrect offsets ... some data may have been missed`.

**Por que só em multi-host.** O protocolo da §5 **recria o tópico** a cada run para zerar
o estado. No host único, o `docker compose down` entre sessões limpava também o volume do
Kafka. No cluster, o broker fica **de pé** entre os 15 runs, e o `kafka-data` (KRaft
single-broker) **retém metadados de offset** de um tópico mesmo após `delete`+`create` de
um tópico **homônimo** — então o Spark, com `startingOffsets=earliest`, encontrava
offsets antigos apontando para além do início do log recriado e concluía que não havia o
que ler.

**Correção.** **Um tópico único por run**: `taxi_trips_stream_<run_id>`. Como o nome nunca
se repete, o tópico é sempre **pristino**, sem metadado de offset herdado. O `reset_state`
passou a **só criar** (sem `delete` assíncrono), o que ainda é mais rápido; o
`run_benchmarks_swarm.sh` exporta `KAFKA_TOPIC` para o producer e passa `--topic` para o
`stream_job`, mantendo as duas pontas sincronizadas no mesmo tópico.

---

Os seis, juntos, contam uma lição de engenharia distribuída: **o que era um recurso único
e implícito no host (disco, filesystem, usuário, broker) vira, no cluster, um recurso que
precisa ser explicitamente compartilhado ou isolado.** Disco (EBS), cache (Ivy), estado
(NFS) e offsets (tópico por run) cada um exigiu tornar explícito um compartilhamento que
antes o `localhost` dava de graça.

### 7.4. Resultados AWS

A matriz completa (15 runs, 3 repetições por configuração, `MAX_RECORDS=1M`, painel em
[`AWS_painel.png`](benchmarks/results/figs/AWS_painel.png)) está em
[`summary_AWS.csv`](benchmarks/results/summary_AWS.csv). Note que os cenários foram
**ajustados ao hardware**: cada t3.medium tem só **2 vCPU**, então o Cenário A varia
`cores ∈ {1, 2}` (não `{1,2,4}` como no host de 12 cores) e o Cenário B fixa 2 cores por
executor. Isso é uma diferença de escala do substrato, não de método.

#### Cenário A — escala vertical (variar cores, 1 worker)

| cores | throughput (reg/s) | latência micro-batch (ms) |
|---:|---:|---:|
| 1 | 19.965,5 ± 990,9 | 50.167,3 ± 2.443,6 |
| 2 | 24.915,4 ± 260,2 | 40.138,7 ± 420,8 |

**Interpretação.** Dentro de **uma** VM, dobrar os cores (1→2) eleva o throughput de
19.965 para 24.915 reg/s — um ganho de **1,25×** — e derruba a latência de micro-batch de
50,2 s para 40,1 s. O ganho existe e é real (o desvio de 2 cores é minúsculo, 1,0 % da
média), mas é **sublinear** (2× de cores → 1,25× de vazão), coerente com a fração serial
do shuffle (§2.6) que a §6.2 já identificara como limite de Amdahl. O paralelismo
intra-nó ainda paga, apenas com menos folga que no host de 12 cores.

#### Cenário B — escala horizontal (variar workers, 2 cores cada) — o foco

| workers | throughput (reg/s) | latência micro-batch (ms) |
|---:|---:|---:|
| 1 | 17.435,6 ± 1.864,5 | 59.712,2 ± 6.589,8 |
| 2 | **26.783,1 ± 1.430,5** | **37.410,0 ± 2.050,4** |
| 3 | 24.454,5 ± 303,5 | 40.896,5 ± 511,0 |

**Interpretação — a resposta que a Fase Local não conseguia dar.** Aqui, adicionar
workers **aumenta o throughput**: de 1 para 2 workers ele sobe de 17.436 para 26.783
reg/s, um ganho de **+54 %** (1,54×), com a latência caindo de 59,7 s para 37,4 s. Isso
decide diretamente entre as duas hipóteses da §7.1: como cada worker está numa **VM física
com CPU própria**, o segundo worker de fato **adiciona capacidade** — logo o limite do
host único era **(2) saturação de hardware**, não (1) uma incapacidade do pipeline de
escalar. **O pipeline escala horizontalmente de verdade quando há hardware novo para
ocupar.**

Mas o ganho **não é ilimitado**: de 2 para 3 workers o throughput **regride** para 24.455
reg/s (−8,7 % em relação a 2). O sweet spot distribuído é **2 workers**. A regressão de
2→3 tem causas concretas, todas do lado do custo de coordenação que a §2.6 antecipa e que
a §8 detalha: as **12 partições** do tópico concentradas em **4 chaves** ativas (§6.4) não
oferecem 3 fluxos equilibrados para paralelizar; o **shuffle passa a atravessar a rede
overlay** (VXLAN) entre uma terceira VM; e as t3 são **burstable** — sob carga sustentada
podem sofrer _throttling_ por esgotamento de créditos de CPU. Com 3 workers, o overhead
marginal de coordenar mais um nó supera o ganho marginal de CPU dele.

**Uma nota de transparência sobre o volume por run.** O
[`runs_AWS.csv`](benchmarks/results/runs_AWS.csv) mostra que nem todos os runs
processaram exatamente o mesmo `total_input_rows`: a maioria registrou 2.000.000 (o volume
fixo drenado nas 2 queries), mas alguns runs de `workers=1` registraram 2.100.000 ou
1.050.000. Isso decorre de o tamanho do micro-batch não ser fixo (§5.1) combinado ao
momento em que o `--idle-stop` fecha o job — em runs mais lentos (1 worker), um batch a
mais ou a menos entra na contagem. **Isso não invalida a comparação** pela razão dada na
§5.1: o **throughput é uma taxa** (linhas ÷ tempo), não um total — ele normaliza pelo
volume efetivamente processado, então comparar taxas entre configurações permanece
legítimo mesmo com o volume variando um pouco por run. É também por isso que reportamos o
**desvio padrão** (§5.3): o desvio maior de `workers=1` (10,7 % da média) reflete
exatamente essa variabilidade, e o desvio muito menor de `workers=2` e `3` (5,3 % e 1,2 %)
mostra que o **ganho de 1→2 é real e não artefato** — as barras não se sobrepõem dentro de
suas margens.

## 8. Análise Comparativa

Esta seção põe as duas fases lado a lado para isolar o que era **limite do hardware
local** e o que é **limite intrínseco do shuffle distribuído** — a pergunta que a §6.4
deixou explicitamente para cá. O foco é o **Cenário B** (escala horizontal), o único
cenário que muda de significado entre os dois ambientes: no host único, "worker" = um
processo a mais na mesma CPU; na AWS, "worker" = uma máquina física a mais.

### 8.1. Cenário B lado a lado, com speedup relativo

O speedup é calculado **relativo a 1 worker dentro de cada ambiente** (throughput(N) ÷
throughput(1)), para medir o **ganho de escala** de cada substrato independentemente do
seu nível absoluto:

| workers | Host único (12 cores) | speedup | Cluster AWS (4 VMs) | speedup |
|---:|---:|:---:|---:|:---:|
| 1 | 46.084 reg/s | 1,00× | 17.436 reg/s | 1,00× |
| 2 | 46.508 reg/s | **1,01×** (estagnou) | 26.783 reg/s | **1,54×** (escala) |
| 3 | 42.648 reg/s | **0,93×** (piorou) | 24.455 reg/s | **1,40×** (regride de 2→3) |

_(Painel comparativo em
[`compare_B_painel.png`](benchmarks/results/figs/compare_B_painel.png): throughput,
latência e speedup relativo lado a lado.)_

### 8.2. O achado central

O painel de **speedup relativo** de
[`compare_B_painel.png`](benchmarks/results/figs/compare_B_painel.png) (terceiro gráfico)
é a imagem que resume o trabalho inteiro: **duas curvas partindo de 1,00× e divergindo.**

- **No host único, adicionar workers satura.** A curva cinza fica **colada em 1,0×**: de
  1→2 workers sobe irrisórios 1 % (dentro do desvio, estatisticamente indistinguível) e de
  2→3 **cai para 0,93×**. É a assinatura da saturação da §6.3: 3 workers × 4 cores = os 12
  cores físicos, e a partir daí "mais um worker" só acrescenta **contenção de CPU e custo
  de coordenação/shuffle** sobre um hardware sem folga.

- **No cluster AWS, adicionar workers escala.** A curva azul **sobe para 1,54× em 2
  workers** — a mesma ação (adicionar um worker) que era inútil no host produz +54 % de
  vazão aqui, porque o worker novo traz uma **VM física com CPU própria**. Este é o
  resultado central do trabalho: **a escala horizontal do pipeline é real; o que a
  mascarava na Fase 1 era o host único, não o pipeline.** A pergunta da §7.1 fica decidida
  em favor da hipótese (2).

A regressão de 2→3 workers no AWS (1,54×→1,40×) não contradiz isso — ela mostra que a
escala horizontal, embora real, **tem um teto**, atingido antes no cluster pequeno por
razões que a §8.4 detalha.

### 8.3. Por que o AWS é mais lento em absoluto — e por que isso não invalida nada

Um leitor apressado notaria que o host único é **mais rápido em números absolutos** (por
exemplo, em `workers=2`: ~46.508 vs ~26.783 reg/s) e concluiria que "a AWS é pior". Seria
uma leitura errada, por confundir **nível absoluto** com **capacidade de escala** — que
são coisas distintas e é a segunda que o trabalho investiga.

O host único é mais rápido em absoluto por um motivo estrutural: **tudo nele é local.** O
shuffle entre executores é _loopback_ (não atravessa placa de rede física); o state store
e o output vão para o **mesmo disco** local; não há NFS nem VXLAN no caminho. O cluster
AWS, ao contrário, paga o **custo real da distribuição**:

- o **shuffle atravessa a rede overlay VXLAN** entre VMs (encapsulamento + rede física
  real, não loopback);
- o **state store e o output vão por NFS** (§7.3, problema 5) — cada escrita de estado
  cruza a rede até o node-infra;
- os executores rodam em **t3.medium (2 vCPU)**, mais fracas que os cores do host de
  desenvolvimento.

Ou seja, o host único é mais rápido **porque simula** a distribuição sem pagar seus
custos; o AWS é mais lento **porque distribui de verdade** e paga cada centavo. Isso **não
invalida o resultado** porque o objetivo declarado (§1.5) nunca foi **bater o número
absoluto** do host único — foi **demonstrar escala horizontal real**. E é justamente o
ambiente mais lento em absoluto que **exibe** a escala (curva azul subindo), enquanto o
mais rápido em absoluto **não a exibe** (curva cinza plana). O número absoluto mede o
substrato; o **speedup** mede o que perguntamos. A distribuição real vence a simulada
exatamente onde importa.

### 8.4. Limites remanescentes do AWS

Reconhecer o teto do cluster é parte do rigor. A escala horizontal do AWS é real mas
**regride de 2→3 workers**, e três fatores concretos, todos do lado do **custo de mover e
coordenar dados** (§2.6), explicam por quê:

- **Partições do tópico = 12 + `key = service_type`.** O paralelismo de consumo do Spark
  é limitado pelo número de partições (12) e, pior, pela **chave**: como o producer usa
  `key = service_type` (§4.1) e só há **quatro** serviços, os eventos se concentram em no
  máximo **4 partições ativas**, com volumes muito díspares (`fhvhv` domina, §1.2). O
  shuffle não tem 12 fluxos equilibrados para distribuir entre 3 workers — tem uns poucos
  grandes e desbalanceados. Um terceiro worker recebe pouco trabalho novo, mas ainda cobra
  seu custo de coordenação. (É a mesma ressalva da §6.4, agora amplificada pela rede.)

- **Shuffle pela rede.** Com 3 workers, reagrupar por chave (janela/zona) força dados a
  atravessar a **overlay VXLAN** entre uma terceira VM. Cada nó adicional aumenta o número
  de pares que precisam trocar dados no shuffle — o custo de coordenação **cresce com o
  número de nós** (§2.6), e a partir de certo ponto supera o ganho de CPU do nó extra.
  Somado ao NFS do state store (§7.3), o terceiro worker paga mais rede do que rende.

- **Throttling das t3 (burstable).** As instâncias t3 são **burstable**: entregam CPU
  plena apenas enquanto têm **créditos**, acumulados quando ociosas. Sob a carga sustentada
  de 15 runs consecutivos, os créditos podem se esgotar e a AWS **throttla** a CPU ao nível
  _baseline_ da instância — reduzindo o ganho efetivo de adicionar uma VM e introduzindo
  variabilidade. Um cluster de instâncias de CPU dedicada (ex.: família `c`) provavelmente
  empurraria o sweet spot para além de 2 workers.

Nenhum desses limites contradiz o achado central — eles o **contextualizam**: a escala
horizontal existe e é mensurável (1→2 = +54 %), e seu teto neste cluster pequeno é
consequência de escolhas de dimensionamento (partições, chave, tipo de VM), não de uma
falha do pipeline. Aumentar as partições ativas (uma chave mais granular), usar VMs de CPU
dedicada e um número maior de nós são os caminhos naturais para empurrar esse teto — e são
justamente as recomendações que a Conclusão (§9) retoma.

## 9. Conclusão

### 9.1. O que foi demonstrado

Este trabalho construiu e avaliou empiricamente um **pipeline distribuído de
processamento de fluxo** sobre dados reais de mobilidade urbana (NYC TLC Trip Record
Data), cumprindo os quatro objetivos declarados na §1.5.

- **O problema é genuinamente Big Data e a arquitetura é adequada a ele.** As três
  dimensões da §1.3 se confirmaram na prática: o volume (≈48,8M linhas / 1,07 GB)
  não cabe na RAM de um nó e forçou leitura em lotes (§4.1); a velocidade foi
  tratada como um fluxo contínuo de alta vazão via Kafka (§4.1, §4.3); e o custo de
  **shuffle** das agregações _stateful_ (§2.6) se revelou, nos números, o gargalo
  que exige distribuição (§6.3, §8). A stack Kafka + Spark Structured Streaming em
  contêineres (§3, §4) sustentou o pipeline nos dois substratos sem mudança de
  lógica — só de orquestração (Compose → Swarm).

- **Dados reais heterogêneos foram unificados sem perder a semântica de event-time.**
  Os quatro serviços, com schemas divergentes (§1.2), foram normalizados para um
  **evento canônico** (§3.2) que deu ao Spark um único `pickup_datetime` sobre o qual
  aplicar watermark e janelamento — a decisão de engenharia de dados que viabilizou
  todo o processamento de stream.

- **O pipeline escala com o hardware — e sabemos sob quais condições.** Este é o
  resultado central. Distinguimos empiricamente **escala vertical** (mais cores por
  nó) de **escala horizontal** (mais nós), com rigor estatístico (3 repetições,
  média e desvio padrão amostral, §5.3). A escala vertical funcionou nos dois
  ambientes (§6.2, §7.4). A escala horizontal foi a pergunta decisiva: no host único
  ela **saturou** (§6.3), mas o experimento na AWS (§7, §8) provou que essa saturação
  era **limite do hardware compartilhado, não do pipeline** — em VMs físicas
  distintas, adicionar um worker de fato **aumentou a vazão** (+54 % de 1 para 2
  workers, §7.4, §8.2). A curva de _speedup_ divergente entre os dois ambientes
  (§8.2) é a evidência que sintetiza o trabalho.

- **O shuffle é o gargalo prático, e mostramos onde e por quê.** Em ambos os
  ambientes, o ganho de paralelismo satura quando o **custo de mover e coordenar
  dados** passa a dominar o custo de computá-los (§6.3, §8.4) — exatamente o
  comportamento que a fundamentação da §2.6 antecipa. O gargalo migra de CPU para
  coordenação/shuffle, e é isso que define o teto de cada substrato.

Em síntese: **a escala horizontal do pipeline é real; o que a mascarava na avaliação
local era o host único, não o desenho da solução** — e isso foi demonstrado com um
protocolo reproduzível e métricas definidas operacionalmente, não com números soltos.

### 9.2. Limitações do trabalho

Coerente com o rigor do relatório, registramos abertamente o que restringe o alcance
das conclusões:

- **Host único vs. cluster pequeno.** A Fase 1 roda tudo em uma máquina (12 cores),
  onde "escala horizontal" é simulada por processos concorrentes (§6.4); a Fase 2
  usa apenas **4 VMs** (3 workers), um cluster pequeno cujo teto é atingido cedo
  (§8.4). Nenhum dos dois é um cluster de produção de dezenas de nós.
- **Instâncias t3 _burstable_.** As VMs worker (t3.medium) entregam CPU plena apenas
  enquanto têm créditos; sob carga sustentada podem sofrer _throttling_ (§8.4),
  adicionando variabilidade e antecipando a saturação horizontal.
- **Partições e chave de particionamento.** O tópico tem 12 partições, mas a chave
  `service_type` (§4.1) tem só quatro valores, concentrando os eventos em no máximo
  4 partições ativas e desbalanceadas (`fhvhv` domina) — o que limita o paralelismo
  de consumo e o shuffle (§6.4, §8.4).
- **Volume do subconjunto.** Usamos ≈48,8M linhas (dois meses), não os ≈102M do
  dataset completo (§1.2), por reprodutibilidade dentro dos recursos disponíveis.
- **Latência de processamento, não end-to-end.** A métrica de latência mede o tempo
  de processamento de um micro-batch pelo Spark, não o atraso percebido por um
  evento do mundo real (§5.1); e, como o tamanho do micro-batch não é fixo, ela é
  uma média sobre lotes grandes e de tamanhos variáveis (§5.1, §6.2).

### 9.3. Trabalhos futuros

Os caminhos naturais para estender o trabalho decorrem diretamente das limitações e
dos limites remanescentes identificados na §8.4:

- **Cluster maior e de CPU dedicada.** Repetir o Cenário B com mais nós e instâncias
  de CPU dedicada (ex.: família `c` da AWS, sem _throttling_) para localizar o
  verdadeiro teto da escala horizontal — o sweet spot atual (2 workers) muito
  provavelmente se moveria adiante.
- **Rebalancear a chave de particionamento.** Trocar `key = service_type` por uma
  chave mais granular (ex.: `pu_location_id`, ou uma combinação) distribuiria os
  eventos pelas 12 partições de forma equilibrada, dando ao shuffle fluxos paralelos
  reais — provavelmente o ganho de maior impacto para a escala.
- **Semântica _exactly-once_.** Evoluir do `acks=1` at-least/at-most-once (§4.1) para
  entrega _exactly-once_ fim a fim (produtor idempotente/transacional + sink
  transacional), quando a aplicação exigir contagens exatas em vez de estatísticas.
- **Processamento em tempo real de verdade.** Limitar o tamanho do micro-batch
  (`maxOffsetsPerTrigger`) e produzir a uma taxa controlada (`--rate-limit`, §4.1)
  para medir **latência end-to-end** por evento, complementando a latência de
  processamento reportada aqui.
- **Dataset completo e mais janelas analíticas.** Escalar para os ≈102M de linhas e
  acrescentar agregações (janelas deslizantes de demanda, detecção de anomalias de
  tráfego) que estressem o estado _stateful_ de formas adicionais.

---
