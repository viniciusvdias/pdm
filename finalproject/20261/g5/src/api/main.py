"""
api-gateway: API REST de recomendacao de filmes via Neo4j.

Endpoints:
  GET /health                          verifica conexao com o Neo4j
  GET /recommend?userId=1&limit=10     WORKLOAD-1: recomendacao por genero
  GET /recommend/collaborative?userId=1&limit=10  WORKLOAD-2: filtragem colaborativa
  GET /movies?limit=20                 lista filmes do grafo
  GET /users/{userId}/ratings          avaliacoes de um usuario
"""

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# ── conexao ───────────────────────────────────────────────────────────

URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
USER     = os.getenv("NEO4J_USER",     "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "g5password")

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    return _driver


def wait_for_neo4j(retries=20, delay=5):
    for attempt in range(1, retries + 1):
        try:
            get_driver().verify_connectivity()
            print(f"[ok] Neo4j conectado (tentativa {attempt})")
            return
        except ServiceUnavailable:
            print(f"[..] Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Neo4j nao respondeu apos multiplas tentativas.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    wait_for_neo4j()
    yield
    if _driver:
        _driver.close()


# ── app ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="G5 Movie Recommender API",
    description="Recomendacao de filmes usando grafo Neo4j (MovieLens 25M)",
    version="1.0.0",
    lifespan=lifespan,
)


# ── helpers ───────────────────────────────────────────────────────────

def run_query(query: str, params: dict) -> list[dict]:
    with get_driver().session() as session:
        result = session.run(query, **params)
        return [record.data() for record in result]


# ── endpoints ─────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        get_driver().verify_connectivity()
        counts = run_query(
            "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS total",
            {},
        )
        return {"status": "ok", "neo4j": URI, "graph": counts}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/movies")
def list_movies(limit: int = Query(default=20, ge=1, le=200)):
    """Lista filmes cadastrados no grafo."""
    rows = run_query(
        """
        MATCH (m:Movie)-[:BELONGS_TO]->(g:Genre)
        RETURN m.movieId AS movieId, m.title AS title,
               collect(g.name) AS genres
        ORDER BY m.movieId
        LIMIT $limit
        """,
        {"limit": limit},
    )
    return {"total": len(rows), "movies": rows}


@app.get("/users/{user_id}/ratings")
def user_ratings(user_id: int, limit: int = Query(default=20, ge=1, le=200)):
    """Retorna as avaliacoes de um usuario."""
    rows = run_query(
        """
        MATCH (u:User {userId: $uid})-[r:RATED]->(m:Movie)
        RETURN m.movieId AS movieId, m.title AS title, r.rating AS rating
        ORDER BY r.rating DESC
        LIMIT $limit
        """,
        {"uid": user_id, "limit": limit},
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Usuario {user_id} nao encontrado.")
    return {"userId": user_id, "total": len(rows), "ratings": rows}


@app.get("/recommend")
def recommend_by_genre(
    userId: int = Query(..., description="ID do usuario"),
    limit: int  = Query(default=10, ge=1, le=50),
):
    """
    WORKLOAD-1 — Recomendacao por genero compartilhado.

    Encontra filmes nao avaliados pelo usuario que compartilham generos
    com filmes que ele avaliou com nota >= 4.0.
    Ordena por forca de recomendacao (numero de generos compartilhados).
    """
    rows = run_query(
        """
        MATCH (u:User {userId: $uid})-[r:RATED]->(m1:Movie)-[:BELONGS_TO]->(g:Genre)
        WHERE r.rating >= 4.0
        MATCH (m2:Movie)-[:BELONGS_TO]->(g)
        WHERE NOT (u)-[:RATED]->(m2)
        WITH m2, collect(DISTINCT g.name) AS sharedGenres, count(*) AS score
        RETURN m2.movieId   AS movieId,
               m2.title     AS title,
               sharedGenres AS genres,
               score        AS recommendationScore
        ORDER BY score DESC
        LIMIT $limit
        """,
        {"uid": userId, "limit": limit},
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhuma recomendacao encontrada para o usuario {userId}. "
                   "Verifique se ele existe e possui avaliacoes >= 4.0.",
        )
    return {
        "workload": "WORKLOAD-1",
        "userId": userId,
        "total": len(rows),
        "recommendations": rows,
    }


@app.get("/recommend/collaborative")
def recommend_collaborative(
    userId: int = Query(..., description="ID do usuario"),
    limit: int  = Query(default=10, ge=1, le=50),
):
    """
    WORKLOAD-2 — Filtragem colaborativa baseada em usuarios similares.

    Encontra usuarios que avaliaram os mesmos filmes com notas altas
    e recomenda filmes que eles gostaram mas o usuario-alvo nao viu.
    """
    rows = run_query(
        """
        MATCH (u:User {userId: $uid})-[r1:RATED]->(m:Movie)<-[r2:RATED]-(similar:User)
        WHERE r1.rating >= 4.0 AND r2.rating >= 4.0 AND u <> similar
        MATCH (similar)-[r3:RATED]->(rec:Movie)
        WHERE NOT (u)-[:RATED]->(rec) AND r3.rating >= 4.0
        WITH rec, count(DISTINCT similar) AS similarUsers, avg(r3.rating) AS avgRating
        RETURN rec.movieId  AS movieId,
               rec.title    AS title,
               similarUsers AS similarUsersCount,
               round(avgRating, 2) AS avgRating
        ORDER BY similarUsers DESC, avgRating DESC
        LIMIT $limit
        """,
        {"uid": userId, "limit": limit},
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhuma recomendacao colaborativa para o usuario {userId}.",
        )
    return {
        "workload": "WORKLOAD-2",
        "userId": userId,
        "total": len(rows),
        "recommendations": rows,
    }


@app.get("/recommend/tag-genome")
def recommend_by_tag_genome(
    userId: int = Query(..., description="ID do usuario"),
    limit: int  = Query(default=10, ge=1, le=50),
    min_score: float = Query(default=0.5, ge=0.0, le=1.0, description="Score minimo de relevancia da tag"),
):
    """
    WORKLOAD-3 — Recomendacao via Tag Genome (scores de relevancia).

    Identifica as tags mais relevantes dos filmes que o usuario avaliou bem,
    depois encontra filmes nao vistos com alta relevancia nessas mesmas tags.
    Usa os scores do tagdl.csv (genome_2021).
    """
    rows = run_query(
        """
        MATCH (u:User {userId: $uid})-[r:RATED]->(watched:Movie)
        WITH u, collect(watched.movieId) AS watchedIds
        MATCH (u)-[r2:RATED]->(m:Movie)-[ht:HAS_TAG]->(t:Tag)
        WHERE r2.rating >= 4.0 AND ht.score >= $min_score
        WITH u, watchedIds, t, avg(ht.score) AS tagRelevance
        ORDER BY tagRelevance DESC
        LIMIT 20
        MATCH (rec:Movie)-[ht2:HAS_TAG]->(t)
        WHERE NOT rec.movieId IN watchedIds AND ht2.score >= $min_score
        WITH rec,
             count(DISTINCT t)  AS matchedTags,
             avg(ht2.score)     AS avgTagScore
        RETURN rec.movieId            AS movieId,
               rec.title              AS title,
               matchedTags            AS matchedTags,
               round(avgTagScore, 4)  AS avgTagScore
        ORDER BY matchedTags DESC, avgTagScore DESC
        LIMIT $limit
        """,
        {"uid": userId, "limit": limit, "min_score": min_score},
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhuma recomendacao via tag genome para o usuario {userId}.",
        )
    return {
        "workload": "WORKLOAD-3",
        "userId": userId,
        "minScore": min_score,
        "total": len(rows),
        "recommendations": rows,
    }
