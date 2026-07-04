"""
injector-job: le os arquivos do genome_2021 e popula o grafo no Neo4j.
Container efemero — termina apos concluir a ingestao.

Arquivos esperados em DATA_DIR:
  metadata.json   -> array JSON  [{movieId, title, genres, year, imdbId, tmdbId}]
  ratings.json    -> NDJSON      {userId, movieId, rating, timestamp}  (1 por linha)
  tags.json       -> array JSON  [{tagId, tag}]
  tag_count.json  -> array JSON  [{tagId, movieId, count}]
  tagdl.csv       -> CSV         movieId,tagId,score

Variaveis de ambiente:
  NEO4J_URI      bolt://db-neo4j:7687
  NEO4J_USER     neo4j
  NEO4J_PASSWORD g5password
  DATA_DIR       /data
  BATCH_SIZE     500  (opcional)
"""

import csv
import json
import os
import sys
import time
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# ── configuracao ──────────────────────────────────────────────────────

URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
USER     = os.getenv("NEO4J_USER",     "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "g5password")
DATA_DIR = os.getenv("DATA_DIR",       "/data")
BATCH    = int(os.getenv("BATCH_SIZE", "500"))

# ── utilidades ────────────────────────────────────────────────────────

def wait_for_neo4j(driver, retries=20, delay=5):
    for attempt in range(1, retries + 1):
        try:
            driver.verify_connectivity()
            print(f"[ok] Neo4j pronto (tentativa {attempt})")
            return
        except ServiceUnavailable:
            print(f"[..] Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)
    print("[erro] Neo4j nao respondeu. Abortando.")
    sys.exit(1)


def load_json_array(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"[aviso] {filename} nao encontrado, pulando.")
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iter_ndjson(filename):
    """Itera linha a linha sobre um arquivo NDJSON (ratings.json)."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"[aviso] {filename} nao encontrado, pulando.")
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def iter_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"[aviso] {filename} nao encontrado, pulando.")
        return
    with open(path, encoding="utf-8") as f:
        yield from csv.DictReader(f)


def batches_from_iter(iterator, size):
    """Agrupa um iterador em lotes de `size` elementos."""
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def run_batch(session, query, rows):
    session.run(query, rows=rows)


# ── etapas de ingestao ────────────────────────────────────────────────

def create_constraints(driver):
    print("[1/6] Criando constraints e indices...")
    stmts = [
        "CREATE CONSTRAINT user_id    IF NOT EXISTS FOR (u:User)  REQUIRE u.userId  IS UNIQUE",
        "CREATE CONSTRAINT movie_id   IF NOT EXISTS FOR (m:Movie) REQUIRE m.movieId IS UNIQUE",
        "CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name    IS UNIQUE",
        "CREATE CONSTRAINT tag_id     IF NOT EXISTS FOR (t:Tag)   REQUIRE t.tagId   IS UNIQUE",
    ]
    with driver.session() as s:
        for stmt in stmts:
            s.run(stmt)
    print("      Constraints criadas.")


def load_tags(driver):
    rows = load_json_array("tags.json")
    if not rows:
        return
    print(f"[2/6] Carregando {len(rows)} tags...")
    query = """
    UNWIND $rows AS row
    MERGE (t:Tag {tagId: toInteger(row.tagId)})
      SET t.name = row.tag
    """
    with driver.session() as s:
        for batch in batches_from_iter(iter(rows), BATCH):
            run_batch(s, query, batch)
    print(f"      Tags carregadas: {len(rows)}")


def load_movies(driver):
    rows = load_json_array("metadata.json")
    if not rows:
        return
    print(f"[3/6] Carregando {len(rows)} filmes e generos...")
    query = """
    UNWIND $rows AS row
    MERGE (m:Movie {movieId: toInteger(row.movieId)})
      SET m.title  = row.title,
          m.year   = toInteger(row.year),
          m.imdbId = row.imdbId,
          m.tmdbId = toInteger(row.tmdbId)
    WITH m, row
    UNWIND split(row.genres, '|') AS genreName
    MERGE (g:Genre {name: genreName})
    MERGE (m)-[:BELONGS_TO]->(g)
    """
    with driver.session() as s:
        for i, batch in enumerate(batches_from_iter(iter(rows), BATCH)):
            run_batch(s, query, batch)
            print(f"      {min((i+1)*BATCH, len(rows))}/{len(rows)} filmes", end="\r")
    print(f"\n      Filmes carregados: {len(rows)}")


def load_ratings(driver):
    print("[4/6] Carregando avaliacoes (NDJSON)...")
    query = """
    UNWIND $rows AS row
    MERGE (u:User  {userId:  toInteger(row.userId)})
    MERGE (m:Movie {movieId: toInteger(row.movieId)})
    MERGE (u)-[r:RATED]->(m)
      SET r.rating    = toFloat(row.rating),
          r.timestamp = toInteger(row.timestamp)
    """
    total = 0
    with driver.session() as s:
        for batch in batches_from_iter(iter_ndjson("ratings.json"), BATCH):
            run_batch(s, query, batch)
            total += len(batch)
            print(f"      {total} avaliacoes processadas", end="\r")
    print(f"\n      Avaliacoes carregadas: {total}")


def load_tag_counts(driver):
    rows = load_json_array("tag_count.json")
    if not rows:
        return
    print(f"[5/6] Carregando {len(rows)} contagens de tags por filme...")
    query = """
    UNWIND $rows AS row
    MATCH (m:Movie {movieId: toInteger(row.movieId)})
    MATCH (t:Tag   {tagId:   toInteger(row.tagId)})
    MERGE (m)-[tc:TAG_COUNT]->(t)
      SET tc.count = toInteger(row.count)
    """
    with driver.session() as s:
        for batch in batches_from_iter(iter(rows), BATCH):
            run_batch(s, query, batch)
    print(f"      Tag counts carregados: {len(rows)}")


def load_tagdl(driver):
    print("[6/6] Carregando scores de relevancia (tagdl.csv)...")
    query = """
    UNWIND $rows AS row
    MATCH (m:Movie {movieId: toInteger(row.movieId)})
    MATCH (t:Tag   {tagId:   toInteger(row.tagId)})
    MERGE (m)-[ht:HAS_TAG]->(t)
      SET ht.score = toFloat(row.score)
    """
    total = 0
    with driver.session() as s:
        for batch in batches_from_iter(iter_csv("tagdl.csv"), BATCH):
            run_batch(s, query, batch)
            total += len(batch)
            print(f"      {total} scores processados", end="\r")
    print(f"\n      Scores carregados: {total}")


def print_summary(driver):
    with driver.session() as s:
        counts = {
            "Users":   s.run("MATCH (u:User)  RETURN count(u) AS c").single()["c"],
            "Movies":  s.run("MATCH (m:Movie) RETURN count(m) AS c").single()["c"],
            "Genres":  s.run("MATCH (g:Genre) RETURN count(g) AS c").single()["c"],
            "Tags":    s.run("MATCH (t:Tag)   RETURN count(t) AS c").single()["c"],
            "Total nos":    s.run("MATCH (n) RETURN count(n) AS c").single()["c"],
            "Total arestas": s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"],
        }
    print("\n=== Resumo do grafo ===")
    for k, v in counts.items():
        print(f"  {k:<16}: {v}")


# ── main ──────────────────────────────────────────────────────────────

def main():
    print("=== injector-job iniciado (genome_2021) ===")
    print(f"  URI      : {URI}")
    print(f"  DATA_DIR : {DATA_DIR}")
    print(f"  BATCH    : {BATCH}")
    print()

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    wait_for_neo4j(driver)

    start = time.time()

    create_constraints(driver)
    load_tags(driver)
    load_movies(driver)
    load_ratings(driver)
    load_tag_counts(driver)
    load_tagdl(driver)

    elapsed = time.time() - start
    print(f"\nIngestao concluida em {elapsed:.1f}s")
    print_summary(driver)

    driver.close()
    print("\n=== injector-job finalizado ===")


if __name__ == "__main__":
    main()
