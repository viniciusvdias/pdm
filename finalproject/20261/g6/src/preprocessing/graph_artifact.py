"""Save/load in-memory Graph CSR arrays for integration fixtures."""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np

from graph.graph import Graph
from preprocessing.load_graph import GraphLoadResult


def graph_artifact_paths(base: Path) -> tuple[Path, Path]:
    """Return ``(npz_path, meta_path)`` for a fixture stem or ``.npz`` path."""
    base = Path(base)
    npz_path = base if base.suffix == ".npz" else base.with_suffix(".npz")
    meta_path = npz_path.with_suffix("").with_suffix(".meta.json")
    return npz_path, meta_path


def save_graph_artifact(
    graph: Graph,
    path: Path,
    *,
    meta: dict,
) -> tuple[Path, Path]:
    """Write compressed CSR arrays + sidecar metadata."""
    npz_path, meta_path = graph_artifact_paths(path)
    npz_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        npz_path,
        node_ids=graph.node_ids,
        indptr=graph.indptr,
        neighbors=graph.neighbors,
        m=np.asarray([graph.m], dtype=np.float64),
    )
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return npz_path, meta_path


def load_graph_artifact(path: Path) -> GraphLoadResult:
    """Load a precomputed graph fixture (fast, no SNAP scan)."""
    npz_path, meta_path = graph_artifact_paths(path)
    if not npz_path.is_file():
        raise FileNotFoundError(npz_path)

    t0 = time.perf_counter()
    with np.load(npz_path) as data:
        graph = Graph.from_csr_arrays(
            node_ids=data["node_ids"],
            indptr=data["indptr"],
            neighbors=data["neighbors"],
            m=float(data["m"][0]),
        )

    meta: dict = {}
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

    elapsed = time.perf_counter() - t0
    fraction_pct = float(meta.get("fraction_pct", 100.0))
    return GraphLoadResult(
        graph=graph,
        load_time_s=elapsed,
        node_count=graph.num_nodes,
        edge_count=int(graph.m),
        fraction_pct=fraction_pct,
    )


def is_graph_artifact(path: Path) -> bool:
    path = Path(path)
    if path.suffix == ".npz":
        return path.is_file()
    return path.with_suffix(".npz").is_file()
