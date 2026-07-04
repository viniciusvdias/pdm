"""Tests for BFS fraction artifact pre-build."""

from pathlib import Path
from unittest.mock import patch

import numpy as np

from graph.graph import Graph
from preprocessing.fraction_artifacts import (
    ensure_fraction_artifact,
    load_fraction_for_benchmark,
)
from preprocessing.load_graph import GraphLoadResult


def test_ensure_fraction_artifact_builds_and_reuses(tmp_path: Path):
    raw = tmp_path / "edges.txt"
    raw.write_text("0 1\n1 2\n2 3\n3 0\n0 2\n", encoding="utf-8")
    artifacts = tmp_path / "artifacts"

    with patch(
        "preprocessing.fraction_artifacts.load_graph_from_snap",
        return_value=GraphLoadResult(
            graph=Graph.from_undirected_edges([(0, 1), (1, 2), (2, 0)]),
            load_time_s=1.5,
            node_count=3,
            edge_count=6,
            fraction_pct=10.0,
        ),
    ) as mock_load:
        first = ensure_fraction_artifact(
            raw,
            fraction_pct=10.0,
            seed=42,
            dataset_slug="orkut",
            artifacts_dir=artifacts,
        )
        assert not first.reused
        assert first.bfs_build_time_s > 0
        assert first.artifact_path.is_file()
        mock_load.assert_called_once()

        second = ensure_fraction_artifact(
            raw,
            fraction_pct=10.0,
            seed=42,
            dataset_slug="orkut",
            artifacts_dir=artifacts,
        )
        assert second.reused
        assert second.bfs_build_time_s == 0.0


def test_load_fraction_for_benchmark_uses_artifact(tmp_path: Path):
    raw = tmp_path / "edges.txt"
    raw.write_text("0 1\n1 2\n", encoding="utf-8")
    artifacts = tmp_path / "artifacts"
    graph = Graph.from_undirected_edges([(0, 1), (1, 2)])
    with patch(
        "preprocessing.fraction_artifacts.load_graph_from_snap",
        return_value=GraphLoadResult(
            graph=graph,
            load_time_s=2.0,
            node_count=3,
            edge_count=4,
            fraction_pct=1.0,
        ),
    ):
        ensure_fraction_artifact(
            raw,
            fraction_pct=1.0,
            seed=7,
            dataset_slug="orkut",
            artifacts_dir=artifacts,
        )

    loaded = load_fraction_for_benchmark(
        raw,
        fraction_pct=1.0,
        seed=7,
        dataset_slug="orkut",
        artifacts_dir=artifacts,
    )
    assert loaded.node_count == 3
    assert loaded.load_time_s < 1.0
    assert np.array_equal(loaded.graph.node_ids, graph.node_ids)
