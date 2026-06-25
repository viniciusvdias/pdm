# RESUMÃO — Trabalho Final PDM 2026/1 · Grupo 5

---

## O QUE O EXERCÍCIO PEDE (checklist de entrega)

### Estrutura de pastas obrigatória ✅
```
g5/
├── bin/           ← scripts executáveis
├── src/           ← código-fonte
├── misc/          ← arquivos auxiliares (k6, índices, resultados)
├── datasample/    ← amostra dos dados < 1MB
├── presentation/  ← PDF dos slides
└── README.md      ← relatório oficial
```

### README.md — 8 seções obrigatórias ✅
| # | Seção | Status |
|---|---|---|
| 1 | Contexto e motivação | ✅ Feito |
| 2 | Descrição dos dados (fonte, schema, volume) | ✅ Feito |
| 3 | Como instalar e rodar | ✅ Feito |
| 4 | Arquitetura (diagrama + containers) | ✅ Feito |
| 5 | Workloads avaliados (3 workloads com queries) | ✅ Feito |
| 6 | Experimentos com ≥ 3 repetições + média ± desvio padrão | ✅ Feito |
| 7 | Limitações e conclusões | ✅ Feito |
| 8 | Referências | ✅ Feito |

### Regras críticas
- ✅ Roda com **apenas `docker compose up --build`** — sem nada mais instalado
- ✅ Datasample < 1MB em `datasample/` (208 KB)
- ✅ Dataset completo **não** está no repo — só link de download
- ✅ Resultados com **média ± desvio padrão** (3 repetições)
- ⏳ PDF dos slides em `presentation/presentation.pdf` ← **FALTA CRIAR**
- ⏳ Pull Request no `viniciusvdias/pdm` ← **FALTA ENVIAR**

---

## O QUE O GRUPO CONSTRUIU

### Tema
**Sistema de Recomendação de Filmes** usando grafo Neo4j com o dataset **genome_2021** (1,38 GB de avaliações reais do MovieLens).

### Tecnologias
| Componente | Tecnologia |
|---|---|
| Banco de dados | Neo4j 5.18 Community (grafo) |
| Ingestão | Python + driver neo4j (injector-job) |
| API REST | Python FastAPI + Uvicorn |
| Orquestração | Docker Compose |
| Testes de carga | k6 (Grafana) |
| Dataset | genome_2021 (JSON + CSV) |

### Dataset — 5 arquivos
| Arquivo | Formato | Conteúdo |
|---|---|---|
| `metadata.json` | JSON array | 9.734 filmes com gêneros, ano, IDs |
| `ratings.json` | **NDJSON** (1/linha) | 25M+ avaliações (1,38 GB) |
| `tags.json` | JSON array | 1.084 tags |
| `tag_count.json` | JSON array | Contagem de tags por filme |
| `tagdl.csv` | CSV | Score de relevância tag↔filme (ML) |

### Grafo no Neo4j
```
(:User)-[:RATED {rating}]  →(:Movie)-[:BELONGS_TO]→(:Genre)
                             (:Movie)-[:HAS_TAG {score}]→(:Tag)
                             (:Movie)-[:TAG_COUNT {count}]→(:Tag)
```

### 3 Formas de recomendar (Workloads)
| Workload | Endpoint | Lógica |
|---|---|---|
| W1 | `/recommend?userId=1` | Filmes com gêneros que o usuário curte |
| W2 | `/recommend/collaborative?userId=1` | Filmes que usuários similares gostaram |
| W3 | `/recommend/tag-genome?userId=1` | Filmes com tags semanticamente relevantes |

---

## O QUE MOSTRAR NA APRESENTAÇÃO

### 1. Terminal — sistema subindo (30 segundos)
```bash
docker compose ps
```
Mostrar `g5-neo4j (healthy)` e `g5-api (healthy)`.

### 2. Terminal — chamadas à API
```bash
curl http://localhost:3000/health
curl "http://localhost:3000/recommend?userId=1"
curl "http://localhost:3000/recommend/collaborative?userId=1"
curl "http://localhost:3000/recommend/tag-genome?userId=1"
```

### 3. Neo4j Browser — visualização do grafo (impacto visual)
Abrir **http://localhost:7474** → usuário `neo4j` / senha `g5password`
```cypher
MATCH (u:User {userId: 1})-[r:RATED]->(m:Movie)-[:BELONGS_TO]->(g:Genre)
RETURN u, r, m, g LIMIT 30
```
Mostra o grafo graficamente com nós e arestas coloridos.

### 4. README.md — mostrar os resultados
Abrir o README e apontar a tabela de resultados com **média ± desvio padrão**.

---

## O QUE FALAR (pontos principais)

### Por que grafo e não SQL?
> "Em SQL, recomendações exigem múltiplos JOINs em tabelas de milhões de linhas — O(n·m). O Neo4j usa *index-free adjacency*: cada aresta sabe exatamente onde o próximo nó está, então a travessia é O(1) por aresta, independente do tamanho do banco."

### Por que o genome_2021 é especial?
> "O diferencial desse dataset é o Tag Genome: scores calculados por machine learning que medem a relevância semântica de cada tag para cada filme. Por exemplo, 'psychological thriller' pode ter score 0.95 para Memento e 0.12 para Toy Story. Isso nos permite recomendar filmes com base em características semânticas, mesmo sem avaliações suficientes de usuários."

### Sobre os resultados
> "Testamos com 50 usuários concorrentes por 2 minutos, repetindo 3 vezes. Os 3 workloads responderam em média abaixo de 35 ms, com taxa de erro de 0% em cerca de 36 mil requisições por run. O throughput ficou em 298 requisições por segundo."

### Por que NDJSON no ratings.json?
> "O arquivo de avaliações tem 1,38 GB. Se fosse um array JSON normal, teríamos que carregar o arquivo inteiro na memória antes de processar o primeiro registro. Com NDJSON — uma linha por objeto JSON — o injector lê e processa linha a linha com memória constante."

### Sobre a arquitetura Docker
> "O projeto roda com um único comando: `docker compose up --build`. O Docker garante a ordem: Neo4j sobe primeiro, o injector espera o banco ficar saudável, carrega os dados e encerra, e só então a API sobe. Nenhuma instalação além do Docker é necessária."

---

## RESULTADOS — números para decorar

| Métrica | Valor |
|---|---|
| WORKLOAD-1 latência | **34,08 ± 2,77 ms** |
| WORKLOAD-2 latência | **33,43 ± 0,23 ms** |
| WORKLOAD-3 latência | **32,58 ± 1,63 ms** |
| Throughput | **298 ± 2,68 RPS** |
| Taxa de erro | **0%** |
| Requisições totais (3 runs) | **~108.000** |
| Nós no grafo (sample) | **305** |
| Arestas no grafo (sample) | **2.492** |
| Tempo de ingestão (sample) | **1,5 segundos** |

---

## O QUE AINDA FALTA FAZER

### 1. Criar o PDF dos slides
- Abrir `presentation/roteiro-tecnico.html` no Chrome
- `Ctrl+P` → Salvar como PDF → salvar como `presentation/presentation.pdf`

### 2. Fazer a entrega (Pull Request)
```bash
# No terminal, dentro da pasta pdm/

# Verificar que apenas g5 está modificado
git status

# Adicionar e commitar
git add finalproject/20261/g5
git commit -m "PDM - final project for 2026/1 - G5"

# Enviar para seu fork
git push origin finalproject-20261-G5
```
Depois abrir o Pull Request no GitHub apontando para `viniciusvdias/pdm` branch `main`.

---

## CHECKLIST FINAL ANTES DE ENTREGAR

- [ ] `docker compose up --build` funciona do zero
- [ ] Os 3 endpoints de recomendação retornam dados
- [ ] Neo4j Browser abre em http://localhost:7474
- [ ] `datasample/` tem os 5 arquivos JSON/CSV (< 1MB)
- [ ] `presentation/presentation.pdf` existe
- [ ] `README.md` tem todas as 8 seções preenchidas
- [ ] Pull Request aberto no `viniciusvdias/pdm`
