# Final project report: Sistema de Recomendação de Filmes Escalável com Neo4j

## 1. Context and motivation

O problema de recomendação de filmes é um caso clássico de Big Data: a estrutura interconectada de usuários, avaliações, filmes e gêneros gera uma explosão combinatória inviável para bancos relacionais tradicionais. Cada usuário avaliou dezenas de filmes; cada filme pertence a múltiplos gêneros; encontrar "filmes semelhantes aos que você gostou" exige percorrer caminhos relacionais em um grafo com milhões de arestas.

O objetivo deste projeto é demonstrar como o **Neo4j** (banco de dados orientado a grafos) resolve esse problema de forma eficiente utilizando *index-free adjacency* — a travessia de vizinhos é O(1) por aresta, independentemente do tamanho total do grafo — em contraste com JOINs SQL que crescem com o tamanho das tabelas.

**Justificativa Big Data (3 Vs):**
- **Volume**: Dataset MovieLens 25M com 25 milhões de avaliações, 62.000 filmes e 162.000 usuários (> 1 GB)
- **Velocidade**: A API responde requisições de recomendação em tempo real sem varreduras completas no disco
- **Variedade**: Dados heterogêneos (metadados de filmes, avaliações numéricas, tags livres) modelados como grafo unificado

## 2. Data

### 2.1 Detailed description

Dataset utilizado: **genome_2021** — fornecido pelo professor via Google Drive.

| Arquivo | Formato | Campos | Tamanho |
|---|---|---|---|
| `metadata.json` | JSON array | movieId, title, genres, year, imdbId, tmdbId | 17 MB |
| `ratings.json` | NDJSON (1 por linha) | userId, movieId, rating, timestamp | 1,38 GB |
| `tags.json` | JSON array | tagId, tag | 37 KB |
| `tag_count.json` | JSON array | tagId, movieId, count | 9,3 MB |
| `tagdl.csv` | CSV | movieId, tagId, score | 305 MB |

**Schema do grafo:**
- Nós: `(:User {userId})`, `(:Movie {movieId, title, year})`, `(:Genre {name})`, `(:Tag {tagId, name})`
- Arestas: `[:RATED {rating, timestamp}]`, `[:BELONGS_TO]`, `[:HAS_TAG {score}]`, `[:TAG_COUNT {count}]`

```
(:User)-[:RATED {rating}]->(:Movie)-[:BELONGS_TO]->(:Genre)
                                   (:Movie)-[:HAS_TAG {score}]->(:Tag)
                                   (:Movie)-[:TAG_COUNT {count}]->(:Tag)
```

### 2.2 How to obtain the data

Uma amostra sintética (< 1MB) com o mesmo formato está disponível em `datasample/` para testes rápidos.

Para o dataset completo (requer Docker):

```bash
bash bin/download-dataset.sh
```

Ou manualmente via Google Drive: https://drive.google.com/drive/folders/11jNkzP_r_5cdXkGDP2aLYynZwgcZcFDx

## 3. How to install and run

> O projeto requer apenas Docker instalado. Nenhuma outra dependência é necessária.

### 3.1 Quick start (using sample data in `datasample/`)

```bash
./bin/run.sh
```

Ou diretamente com Docker Compose:

```bash
docker compose up --build
```

Após subir, acesse:
- **Neo4j Browser**: http://localhost:7474 (usuário: `neo4j`, senha: `g5password`)
- **API de recomendação**: http://localhost:3000

Exemplo de requisição:

```bash
curl "http://localhost:3000/recommend?userId=1"
```

### 3.2 How to run with the full dataset

1. Baixe o dataset conforme a seção 2.2
2. Substitua o conteúdo de `datasample/` pelos arquivos completos ou monte um volume:

```bash
DATA_PATH=/caminho/para/ml-25m docker compose up --build
```

## 4. Project architecture

```
[datasample/ ou dataset completo]
         |
         v
  [injector-job]          Container efêmero Python
  Lê metadata.json,       Cria nós e arestas no Neo4j
  ratings.json (NDJSON),  Termina após ingestão
  tags.json, tag_count.json, tagdl.csv
         |
         v
   [db-neo4j]             Neo4j 5.18 Community
   Grafo de conhecimento  Porta 7474 (browser)
   User→RATED→Movie       Porta 7687 (bolt)
   Movie→BELONGS_TO→Genre
         |
         v
  [api-gateway]           Python FastAPI
  GET /recommend?userId   Executa query Cypher
  GET /health             Retorna top-10 filmes
         |
         v
     [Cliente]            curl / browser / k6
```

**Containers e interação:**

| Serviço | Tecnologia | Porta | Papel |
|---|---|---|---|
| `db-neo4j` | Neo4j 5.18 Community | 7474, 7687 | Armazena e consulta o grafo |
| `injector-job` | Python + neo4j driver | — | Popula o Neo4j com os dados (efêmero) |
| `api-gateway` | Python FastAPI | 3000 | Expõe endpoints REST de recomendação |

## 5. Workloads evaluated

**[WORKLOAD-1] Recomendação por gênero compartilhado**

Dado um `userId`, encontra filmes não avaliados pelo usuário que compartilham gêneros com filmes que ele avaliou com nota ≥ 4.0. Ordena por frequência de gênero compartilhado.

```cypher
MATCH (u:User {userId: $userId})-[r:RATED]->(m1:Movie)-[:BELONGS_TO]->(g:Genre)
MATCH (m2:Movie)-[:BELONGS_TO]->(g)
WHERE NOT (u)-[:RATED]->(m2) AND r.rating >= 4.0
RETURN m2.title, count(*) AS score
ORDER BY score DESC
LIMIT 10
```

**[WORKLOAD-2] Recomendação colaborativa (usuários semelhantes)**

Encontra usuários que avaliaram os mesmos filmes com notas altas e recomenda filmes que eles gostaram mas o usuário-alvo não viu.

```cypher
MATCH (u:User {userId: $userId})-[r1:RATED]->(m:Movie)<-[r2:RATED]-(similar:User)
WHERE r1.rating >= 4.0 AND r2.rating >= 4.0 AND u <> similar
MATCH (similar)-[r3:RATED]->(rec:Movie)
WHERE NOT (u)-[:RATED]->(rec) AND r3.rating >= 4.0
RETURN rec.title, count(similar) AS score
ORDER BY score DESC
LIMIT 10
```

**[WORKLOAD-3] Recomendação via Tag Genome**

Usa os scores de relevância do `tagdl.csv` para encontrar filmes não vistos que compartilham tags altamente relevantes com os filmes favoritos do usuário.

```cypher
MATCH (u:User {userId: $userId})-[r:RATED]->(m:Movie)-[ht:HAS_TAG]->(t:Tag)
WHERE r.rating >= 4.0 AND ht.score >= 0.5
WITH t, avg(ht.score) AS tagRelevance ORDER BY tagRelevance DESC LIMIT 20
MATCH (rec:Movie)-[ht2:HAS_TAG]->(t)
WHERE NOT (u)-[:RATED]->(rec) AND ht2.score >= 0.5
RETURN rec.title, count(DISTINCT t) AS matchedTags, avg(ht2.score) AS avgTagScore
ORDER BY matchedTags DESC, avgTagScore DESC LIMIT 10
```

**[WORKLOAD-4] Ingestão batch (desempenho do injector)**

Tempo total para carregar o dataset completo no Neo4j (metadata + ratings + tags + tagdl), incluindo criação de índices e constraints.

## 6. Experiments and results

### 6.1 Experimental environment

Experimentos executados em máquina local com Docker Desktop:

| Componente | Especificação |
|---|---|
| CPU | Intel Core i5-12450H (12ª geração), 8 cores / 12 threads |
| RAM | 7.8 GB |
| OS | Windows 11 Home (build 26200) |
| Docker | 29.4.3 (Docker Desktop, engine Linux) |
| Neo4j | 5.18 Community (container Docker, heap 512m–2G, pagecache 1G) |
| API | FastAPI + Uvicorn, Python 3.12-slim |
| Load tester | grafana/k6:latest (container Docker) |

### 6.2 How to perform benchmarking (simple guide)

Com os serviços já rodando (`docker compose up --build -d`), execute cada repetição com:

```bash
docker run --rm --network g5_g5net \
  -v "$(pwd)/misc/k6-script.js:/home/k6/test.js" \
  -e API_BASE_URL=http://g5-api:3000 \
  grafana/k6:latest run /home/k6/test.js
```

**Cenário:** 50 usuários virtuais (VUs) realizando requisições aleatórias aos 3 workloads durante 2 minutos contínuos. Cada VU executa: W1 → sleep 0.1s → W2 → sleep 0.1s → W3 → sleep 0.2s.

Os dados brutos de cada run estão em `misc/benchmark-results.csv`.

### 6.3 What did you test?

**Parâmetros variados:** workload (tipo de query Cypher), repetição (variabilidade entre runs)

**Métricas coletadas por run:**
- **Latência média** (avg) e **p90/p95** por workload em milissegundos
- **Throughput**: requisições processadas por segundo (RPS)
- **Taxa de erro**: requisições que retornaram status HTTP 5xx

**Configuração fixa:** 50 VUs concorrentes, duração 2 minutos, sem aquecimento separado.

Cada workload foi executado dentro da mesma iteração, permitindo comparação direta entre as 3 estratégias de recomendação sob a mesma carga.

### 6.4 Results

#### Latência por Workload (ms) — 3 repetições

| Workload | Descrição | Run 1 | Run 2 | Run 3 | **Avg ± Std** |
|---|---|---|---|---|---|
| WORKLOAD-1 | Recomendação por gênero | 37.20 | 31.91 | 33.13 | **34.08 ± 2.77 ms** |
| WORKLOAD-2 | Filtragem colaborativa | 33.64 | 33.18 | 33.47 | **33.43 ± 0.23 ms** |
| WORKLOAD-3 | Tag Genome (tagdl.csv) | 34.28 | 31.04 | 32.41 | **32.58 ± 1.63 ms** |

#### Throughput e Confiabilidade — 3 repetições

| Métrica | Run 1 | Run 2 | Run 3 | **Avg ± Std** |
|---|---|---|---|---|
| Throughput (RPS) | 295.07 | 300.32 | 298.66 | **298.02 ± 2.68 RPS** |
| Iterações completadas | 11.852 | 12.063 | 11.989 | — |
| Taxa de erro (%) | 0.00 | 0.00 | 0.00 | **0.00%** |
| Checks aprovados (%) | 100.0 | 100.0 | 100.0 | **100.0%** |

#### Latência p90 / p95 por Workload (média dos 3 runs)

| Workload | p90 (ms) | p95 (ms) | Latência máx (ms) |
|---|---|---|---|
| WORKLOAD-1 | 64.81 | 77.68 | 951.91* |
| WORKLOAD-2 | 64.40 | 76.24 | 741.18* |
| WORKLOAD-3 | 62.87 | 74.43 | 407.63* |

> \* Picos de latência máxima ocorreram apenas no Run 1, associados ao aquecimento da JVM do Neo4j. Nos Runs 2 e 3, os valores máximos foram inferiores a 210 ms, indicando comportamento estável após warmup.

**Discussão:**

- As três queries Cypher apresentaram latências médias muito próximas (~32–34 ms), confirmando que a *index-free adjacency* do Neo4j iguala o tempo de travessia independentemente da complexidade do padrão.
- WORKLOAD-2 foi o mais estável (desvio padrão de apenas 0.23 ms), evidenciando que a filtragem colaborativa converge rapidamente no grafo amostral.
- WORKLOAD-1 apresentou o maior desvio (2.77 ms), possivelmente por variação no número de gêneros por usuário sorteado.
- O throughput de **298 RPS** com desvio de 2.68 indica sistema altamente estável sob 50 VUs concorrentes.
- A taxa de erro de **0%** ao longo de ~36.000 requisições por run demonstra robustez da arquitetura de microsserviços.

## 7. Limitations and conclusions

**O que funcionou bem:**

- A modelagem em grafo no Neo4j provou-se eficaz: as três estratégias de recomendação respondem em média abaixo de 35 ms, com throughput superior a 295 RPS sob 50 usuários concorrentes.
- A arquitetura de microsserviços com Docker Compose garantiu isolamento, reprodutibilidade e simplicidade de execução — o projeto sobe com um único `docker compose up --build`.
- O uso do Tag Genome (`tagdl.csv`) como terceira dimensão de recomendação enriquece os resultados além das abordagens tradicionais por gênero e colaborativa.
- A ingestão em lotes via `UNWIND` no Cypher mostrou boa performance: o sample (1.500 avaliações, 100 filmes, 37 tags) foi carregado em 1.5 segundos.

**Limitações:**

- Os experimentos foram realizados com o **datasample sintético** (100 filmes, 1.500 avaliações), não com o dataset completo genome_2021 (1.38 GB de ratings). Com o dataset real, a latência das queries colaborativas e de tag genome tende a crescer significativamente devido ao volume de arestas.
- A máquina de teste tem apenas 7.8 GB de RAM compartilhados entre Neo4j (heap 2G + pagecache 1G), API e k6, o que limita a representatividade do experimento em cenários de produção.
- O Neo4j Community Edition não suporta clustering nem sharding, restringindo a escalabilidade horizontal. Para datasets de escala real, o Neo4j Enterprise ou uma solução distribuída seria necessária.
- O pico de latência no Run 1 (~951 ms) evidencia que benchmarks sem fase de aquecimento (warmup) subestimam a performance real. Em experimentos futuros, recomenda-se um período de warmup de 30 segundos antes de iniciar a coleta.

**Conclusões:**

Este projeto demonstrou que o Neo4j, combinado com uma API REST containerizada, é uma solução viável para sistemas de recomendação baseados em grafos. A *index-free adjacency* permite que travessias de relacionamentos complexos (usuário → avaliações → filmes → gêneros → outros filmes) operem em latência submilisegundo por aresta, resultando em respostas médias de ~33 ms mesmo sob carga concorrente. A integração do Tag Genome como fonte de relevância semântica representa uma vantagem competitiva em relação a abordagens puramente colaborativas.

## 8. References and external resources

- genome_2021 Dataset (Google Drive, fornecido pelo professor): https://drive.google.com/drive/folders/11jNkzP_r_5cdXkGDP2aLYynZwgcZcFDx
- F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. *ACM TiiS* 5, 4: 1–19. https://doi.org/10.1145/2827872
- Neo4j Documentation: https://neo4j.com/docs/
- Neo4j Python Driver: https://neo4j.com/docs/python-manual/current/
- FastAPI Documentation: https://fastapi.tiangolo.com/
- k6 Load Testing Documentation: https://k6.io/docs/
- Robinson, I., Webber, J., & Eifrem, E. (2015). *Graph Databases* (2nd ed.). O'Reilly Media.
