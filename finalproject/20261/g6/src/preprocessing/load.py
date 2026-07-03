"""Load graph from SNAP TXT or precomputed CSR fixture."""

from __future__ import annotations

from pathlib import Path

from preprocessing.graph_artifact import is_graph_artifact, load_graph_artifact
from preprocessing.load_graph import GraphLoadResult, load_graph_from_snap


def load_graph(path: Path, *, fraction_pct: float = 100.0, seed: int = 42, directed: bool = False) -> GraphLoadResult:
    """Dispatch to artifact loader (``.npz``) or SNAP edge-list loader."""
    path = Path(path)
    if is_graph_artifact(path):
        return load_graph_artifact(path)
    return load_graph_from_snap(path, fraction_pct=fraction_pct, seed=seed, directed=directed)
