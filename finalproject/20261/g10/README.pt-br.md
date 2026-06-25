<div align="center">

🇺🇸 [English](README.md) &nbsp;|&nbsp; 🇧🇷 Português

</div>

---

# Relatório do Projeto Final: Wikimedia Real-Time Topic Analytics

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

**Discussão**: a CPU escala linearmente de `local[1]` para `local[2]` (~104% → ~216%), mostrando que o pipeline usa dois núcleos completos efetivamente quando disponíveis. Porém, ir de `local[2]` para `local[4]` não produz nenhum ganho mensurável (215,7% → 211,2%). Esta é a confirmação mais clara de que o sistema é **limitado pela entrada a ~29 ev/s**: o stream da Wikimedia não consegue alimentar 4 threads com rapidez suficiente. Dois threads é o ponto de saturação; além disso, threads adicionais somam overhead de escalonamento JVM sem benefício de throughput.

---

#### Experimento 3 — Tamanho da janela de agregação

Todas as execuções: `CLASSIFIER_MODE=embedding`, `SPARK_MASTER=local[*]`, taxonomia de 12 categorias.

| Duração da janela | CPU (%) | RAM (MB) | Execuções |
|---|---|---|---|
| 1 min | 206,7 | 1.961 | 1 |
| 5 min | 212,9 | 2.020 | 1 |
| 10 min | 210,8 | 2.628 | 1 |

**Discussão**: a CPU é praticamente constante em todos os tamanhos de janela (~207–213%), confirmando que a etapa de classificação (WORKLOAD-1) domina o custo de CPU independentemente do tamanho do estado acumulado. A RAM conta a história real: a janela de 10 minutos requer 34% mais memória que a de 1 minuto (+667 MB), causado pelo maior estado de watermark. Para uso em produção, a janela de 5 minutos oferece o melhor equilíbrio: overhead de RAM insignificante (+59 MB) com granularidade útil para detecção de tendências.

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
- **Throughput limitado pela entrada**: a taxa de ~29 ev/s é determinada pelo stream da Wikimedia, não pela capacidade do sistema. Benchmarks com uma fonte sintética controlada dariam um quadro mais limpo do teto real de processamento.
- **Ambiente somente CPU**: o modelo multilíngue (`intfloat/multilingual-e5-base`) não foi avaliado em benchmark. Em hardware com GPU, seria o modelo preferido para produção; em CPU é muito lento para experimentação iterativa.
- **Uma execução por configuração**: cada configuração de benchmark foi executada uma vez (~5 min por config × 11 configs ≈ 1,5 h no total). Desvio padrão não pode ser reportado; os resultados são direcionais.

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
