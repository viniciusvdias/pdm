"""
Semantic classifier for Wikimedia article titles.

Hardware
--------
GPU (recommended):
    NVIDIA RTX 5060 (Blackwell / sm_120) with 8 GB VRAM.
    The Dockerfile ships torch 2.7 built against CUDA 12.8, which is the
    minimum for Blackwell.  'all-MiniLM-L6-v2' uses ~300 MB VRAM;
    'intfloat/multilingual-e5-base' uses ~600 MB — both fit comfortably.

CPU (fallback):
    Any machine with >= 32 GB RAM.  Remove the `deploy.resources` block from
    docker-compose.yaml (or set DEVICE=cpu in .env).  torch.cuda.is_available()
    returns False and the code falls back automatically.  Inference is ~10x
    slower than GPU but handles the Wikimedia event rate without dropping data.

Model selection (MODEL_NAME in .env)
-------------------------------------
  intfloat/multilingual-e5-base  — default; covers all Wikipedia languages;
                                   requires "query: " prefix (handled automatically)
  all-MiniLM-L6-v2               — English-only; 3x faster; good for benchmarks
"""
from __future__ import annotations

import json
import os

import pandas as pd
import torch
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from sentence_transformers import SentenceTransformer, util

MODEL_NAME: str = os.environ.get("MODEL_NAME", "all-MiniLM-L6-v2")
TAXONOMY_PATH: str = os.environ.get("TAXONOMY_PATH", "/misc/topics.json")
ENCODE_BATCH_SIZE: int = int(os.environ.get("ENCODE_BATCH_SIZE", "64"))

# e5 family models require the "query: " prefix at inference time.
_TEXT_PREFIX: str = "query: " if "e5" in MODEL_NAME.lower() else ""

# Auto-select: GPU (RTX 5060 8 GB) when available, else CPU (>= 32 GB RAM).
DEVICE: str = os.environ.get("DEVICE", "cuda" if torch.cuda.is_available() else "cpu")


# ── Driver-side helpers ─────────────────────────────────────────────────────────

def preload_model() -> None:
    """Download / warm the Hugging Face model cache on the driver.

    Call this before starting the Spark streaming query so the first micro-batch
    does not stall on a cold model download.  Uses CPU to avoid holding VRAM
    during Spark initialisation.
    """
    print(f"[classifier] Device selected for inference: {DEVICE}", flush=True)
    print(f"[classifier] Pre-loading model '{MODEL_NAME}' into cache...", flush=True)
    SentenceTransformer(MODEL_NAME, device="cpu")
    print("[classifier] Model cache ready.", flush=True)


def _load_taxonomy() -> tuple[list[str], list[str]]:
    """Return (category_names, description_texts) from the taxonomy JSON."""
    with open(TAXONOMY_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    names = [cat["name"] for cat in data["categories"]]
    texts = [f"{cat['name']}: {cat['description']}" for cat in data["categories"]]
    return names, texts


# ── Worker-side singleton (lazy, one per executor process) ──────────────────────
#
# Spark runs the Pandas UDF body inside each executor's Python process.  We keep
# a single model instance per process to avoid redundant model loads across
# micro-batches.

_model: SentenceTransformer | None = None
_category_vectors: torch.Tensor | None = None
_category_names: list[str] = []


def _get_worker_state() -> tuple[SentenceTransformer, torch.Tensor, list[str]]:
    global _model, _category_vectors, _category_names
    if _model is None:
        _category_names, descriptions = _load_taxonomy()
        _model = SentenceTransformer(MODEL_NAME, device=DEVICE)
        _category_vectors = _model.encode(
            [_TEXT_PREFIX + d for d in descriptions],
            convert_to_tensor=True,
        )
    return _model, _category_vectors, _category_names


# ── Spark Pandas UDF ────────────────────────────────────────────────────────────

@pandas_udf(StringType())
def classify_udf(texts: pd.Series) -> pd.Series:
    """Classify a micro-batch of article titles into taxonomy categories.

    Receives the full 'cleaned_text' column of a Spark micro-batch as a
    pandas.Series, performs a single batched model.encode() call, and returns
    one 'Category|confidence' string per row  (e.g. 'Sports|0.93').

    Empty / whitespace-only inputs return 'Others|0.0' without hitting the model.
    """
    model, category_vectors, cat_names = _get_worker_state()

    s = texts.fillna("").astype(str)
    nonempty = s.str.strip() != ""
    results = pd.Series(["Others|0.0"] * len(s), index=s.index)

    if nonempty.any():
        batch = s[nonempty].tolist()
        embeddings = model.encode(
            [_TEXT_PREFIX + t for t in batch],
            convert_to_tensor=True,
            batch_size=ENCODE_BATCH_SIZE,
        )
        scores = util.cos_sim(embeddings, category_vectors)
        best_idx = torch.argmax(scores, dim=1).tolist()
        best_val = torch.max(scores, dim=1).values.tolist()
        results[nonempty] = [
            f"{cat_names[i]}|{v:.2f}" for i, v in zip(best_idx, best_val)
        ]

    return results
