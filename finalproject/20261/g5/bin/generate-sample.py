"""
Gera dados sinteticos no formato genome_2021 para o datasample/.
Produz: tags.json, metadata.json, ratings.json (NDJSON), tag_count.json, tagdl.csv
Total < 1MB, formato identico ao dataset real do Drive.
"""
import csv
import json
import os
import random
import time

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "datasample")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── dados base ────────────────────────────────────────────────────────

GENRES_LIST = [
    "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir",
    "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
    "Thriller", "War", "Western",
]

TITLES = [
    "Toy Story", "Jumanji", "Heat", "Casino", "Babe",
    "The Usual Suspects", "Shawshank Redemption", "Forrest Gump",
    "Pulp Fiction", "The Silence of the Lambs", "Schindler's List",
    "The Dark Knight", "Inception", "Interstellar", "The Matrix",
    "Goodfellas", "Fight Club", "The Godfather", "Blade Runner",
    "Alien", "Star Wars", "Back to the Future", "Jurassic Park",
    "The Lion King", "Titanic", "Avatar", "The Avengers", "Iron Man",
    "Spider-Man", "Batman Begins", "Memento", "The Departed",
    "American History X", "Gladiator", "Saving Private Ryan",
    "A Beautiful Mind", "Good Will Hunting", "The Truman Show",
    "Eternal Sunshine", "Lost in Translation", "Her", "Arrival",
    "Ex Machina", "Mad Max: Fury Road", "The Martian", "Gravity",
    "Moon", "District 9", "Children of Men", "Pan's Labyrinth",
    "Oldboy", "Spirited Away", "Princess Mononoke", "Akira",
    "Trainspotting", "Requiem for a Dream", "Black Swan", "Whiplash",
    "Birdman", "The Grand Budapest Hotel", "Moonlight", "Parasite",
    "Joker", "1917", "Marriage Story", "The Irishman",
    "Knives Out", "Ford v Ferrari", "Little Women", "Nomadland",
    "Soul", "Tenet", "News of the World", "Mank",
    "Promising Young Woman", "Minari", "The Father", "Judas",
    "Sound of Metal", "Da 5 Bloods", "Ma Rainey", "One Night",
    "Wolfwalkers", "Over the Moon", "Onward", "Greyhound",
    "The Lighthouse", "Uncut Gems", "Marriage Story", "1917",
    "Parasite", "Once Upon a Time", "Avengers Endgame", "Us",
    "Midsommar", "Hereditary", "Get Out", "It Follows",
    "A Quiet Place", "Bird Box", "The Witch", "Annihilation",
]

TAGS_LIST = [
    "classic", "must-watch", "overrated", "underrated", "slow-burn",
    "mind-bending", "emotional", "funny", "scary", "action-packed",
    "visually stunning", "great soundtrack", "based on true story",
    "plot twist", "cult classic", "oscar winner", "dark humor",
    "thought-provoking", "dystopia", "superhero", "time travel",
    "psychological", "surreal", "violent", "inspiring",
    "coming of age", "space", "artificial intelligence", "heist",
    "based on novel", "true crime", "war film", "animation",
    "anthology", "mockumentary", "silent film", "foreign language",
]

N_MOVIES  = 100
N_USERS   = 150
N_RATINGS = 1500
N_TAG_COUNTS = 300
N_TAGDL = 500

BASE_TS = int(time.time()) - 60 * 60 * 24 * 365 * 5

# ── tags.json ─────────────────────────────────────────────────────────

tags = [{"tagId": i + 1, "tag": t} for i, t in enumerate(TAGS_LIST)]
with open(os.path.join(OUTPUT_DIR, "tags.json"), "w", encoding="utf-8") as f:
    json.dump(tags, f, ensure_ascii=False, indent=2)
print(f"tags.json        -> {len(tags)} tags")

# ── metadata.json ─────────────────────────────────────────────────────

metadata = []
for i in range(1, N_MOVIES + 1):
    title_base = TITLES[(i - 1) % len(TITLES)]
    year = random.randint(1980, 2023)
    n_genres = random.randint(1, 3)
    genres = "|".join(random.sample(GENRES_LIST, n_genres))
    metadata.append({
        "movieId": i,
        "title": f"{title_base} ({year})",
        "genres": genres,
        "year": year,
        "imdbId": str(random.randint(100000, 9999999)).zfill(7),
        "tmdbId": random.randint(100, 99999),
    })

with open(os.path.join(OUTPUT_DIR, "metadata.json"), "w", encoding="utf-8") as f:
    json.dump(metadata, f, ensure_ascii=False, indent=2)
print(f"metadata.json    -> {len(metadata)} filmes")

# ── ratings.json (NDJSON — uma linha por registro) ────────────────────

seen = set()
ratings = []
while len(ratings) < N_RATINGS:
    uid = random.randint(1, N_USERS)
    mid = random.randint(1, N_MOVIES)
    if (uid, mid) in seen:
        continue
    seen.add((uid, mid))
    ratings.append({
        "userId": uid,
        "movieId": mid,
        "rating": round(random.choice([0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]), 1),
        "timestamp": BASE_TS + random.randint(0, 60 * 60 * 24 * 365 * 5),
    })

with open(os.path.join(OUTPUT_DIR, "ratings.json"), "w", encoding="utf-8") as f:
    for r in ratings:
        f.write(json.dumps(r) + "\n")
print(f"ratings.json     -> {len(ratings)} avaliações (NDJSON)")

# ── tag_count.json ────────────────────────────────────────────────────

tag_counts = []
seen_tc = set()
while len(tag_counts) < N_TAG_COUNTS:
    tid = random.randint(1, len(TAGS_LIST))
    mid = random.randint(1, N_MOVIES)
    if (tid, mid) in seen_tc:
        continue
    seen_tc.add((tid, mid))
    tag_counts.append({
        "tagId": tid,
        "movieId": mid,
        "count": random.randint(1, 50),
    })

with open(os.path.join(OUTPUT_DIR, "tag_count.json"), "w", encoding="utf-8") as f:
    json.dump(tag_counts, f, indent=2)
print(f"tag_count.json   -> {len(tag_counts)} contagens de tags")

# ── tagdl.csv ─────────────────────────────────────────────────────────

tagdl_rows = []
seen_dl = set()
while len(tagdl_rows) < N_TAGDL:
    mid = random.randint(1, N_MOVIES)
    tid = random.randint(1, len(TAGS_LIST))
    if (mid, tid) in seen_dl:
        continue
    seen_dl.add((mid, tid))
    tagdl_rows.append({
        "movieId": mid,
        "tagId": tid,
        "score": round(random.uniform(0.0, 1.0), 4),
    })

with open(os.path.join(OUTPUT_DIR, "tagdl.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["movieId", "tagId", "score"])
    w.writeheader()
    w.writerows(tagdl_rows)
print(f"tagdl.csv        -> {len(tagdl_rows)} scores de relevância")

# ── tamanho total ─────────────────────────────────────────────────────

total = sum(
    os.path.getsize(os.path.join(OUTPUT_DIR, f))
    for f in os.listdir(OUTPUT_DIR)
    if os.path.isfile(os.path.join(OUTPUT_DIR, f))
)
print(f"\nTotal datasample : {total / 1024:.1f} KB (limite: 1024 KB)")
print(f"Arquivos salvos em: {os.path.abspath(OUTPUT_DIR)}")
