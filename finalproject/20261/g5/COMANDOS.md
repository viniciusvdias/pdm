# Comandos — Grupo 5 PDM

> Sempre rodar dentro da pasta `finalproject/20261/g5/`

---

## SUBIR O PROJETO

```bash
docker compose up --build -d
```
Aguardar ~40s. Verificar:
```bash
docker compose ps
```
✅ `g5-neo4j` → healthy  
✅ `g5-api`   → healthy  
✅ `g5-injector` → não aparece (saiu com sucesso)

---

## TESTAR A API

```bash
# saúde geral
curl http://localhost:3000/health

# recomendação por gênero
curl "http://localhost:3000/recommend?userId=1"

# recomendação colaborativa
curl "http://localhost:3000/recommend/collaborative?userId=1"

# recomendação por tag genome
curl "http://localhost:3000/recommend/tag-genome?userId=1"
```

---

## NEO4J BROWSER

Abrir: **http://localhost:7474**  
Usuário: `neo4j` | Senha: `g5password`

Query para visualizar o grafo:
```cypher
MATCH (u:User {userId: 1})-[r:RATED]->(m:Movie)-[:BELONGS_TO]->(g:Genre)
RETURN u, r, m, g LIMIT 30
```

---

## TESTE DE CARGA (k6)

```bash
docker run --rm --network g5_g5net \
  -v "$(pwd)/misc/k6-script.js:/home/k6/test.js" \
  -e API_BASE_URL=http://g5-api:3000 \
  grafana/k6:latest run /home/k6/test.js
```
Roda 50 usuários por 2 minutos.

---

## PARAR

```bash
docker compose down
```

## PARAR E LIMPAR TUDO (senha travada / recomeçar do zero)

```bash
docker compose down -v
docker compose up --build -d
```
