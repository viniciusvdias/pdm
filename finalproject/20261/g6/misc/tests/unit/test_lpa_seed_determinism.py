"""Distributed LPA produces identical partitions across seeds (expected)."""

import numpy as np

from graph.graph import Graph
from lpa_core.distributed import run_lpa_distributed
from lpa_core.lpa import lpa_iteration_chunk


def _run_with_seed(graph: Graph, seed: int) -> np.ndarray:
    def run_one_iteration(snapshot, chunk_list):
        chunk_results = [
            (chunk, lpa_iteration_chunk(chunk, graph, snapshot))
            for chunk in chunk_list
        ]
        return chunk_results, []

    labels, _, _, _ = run_lpa_distributed(
        graph,
        max_iter=20,
        chunk_divisor=2,
        seed=seed,
        backend="test",
        run_one_iteration=run_one_iteration,
    )
    return labels


def test_distributed_lpa_identical_across_seeds():
    edges = [(i, i + 1) for i in range(9)] + [(0, 5), (5, 9)]
    graph = Graph.from_undirected_edges(edges)
    labels_42 = _run_with_seed(graph, seed=42)
    labels_99 = _run_with_seed(graph, seed=99)
    assert np.array_equal(
        labels_42,
        labels_99,
    ), "synchronous batch LPA is seed-invariant for partitions (expected)"
