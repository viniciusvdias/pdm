"""Tests for graph CSR fixture save/load."""

import json
from pathlib import Path

import numpy as np

from graph.graph import Graph
from preprocessing.graph_artifact import load_graph_artifact, save_graph_artifact


def test_graph_fixture_roundtrip(tmp_path: Path):
    g = Graph.from_undirected_edges([(0, 1), (1, 2), (2, 0)])
    meta = {"dataset_slug": "toy", "fraction_pct": 100.0, "seed": 1}
    stem = tmp_path / "toy"
    save_graph_artifact(g, stem, meta=meta)

    loaded = load_graph_artifact(stem)
    assert loaded.node_count == 3
    assert loaded.edge_count == int(g.m)
    assert np.array_equal(loaded.graph.node_ids, g.node_ids)
    assert np.array_equal(loaded.graph.indptr, g.indptr)
    assert np.array_equal(loaded.graph.neighbors, g.neighbors)

    sidecar = json.loads(stem.with_suffix(".meta.json").read_text(encoding="utf-8"))
    assert sidecar["dataset_slug"] == "toy"
