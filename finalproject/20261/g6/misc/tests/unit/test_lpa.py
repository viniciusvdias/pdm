"""Tests for Label Propagation core."""

import numpy as np
import pytest

from graph.graph import Graph
from lpa_core.lpa import (
    LpaResult,
    format_iter_log,
    format_run_start,
    format_runtime_log,
    format_summary_logs,
    lpa_iteration_chunk,
    node_chunks,
    run_lpa_sequential,
    shuffled_node_chunks,
)
from tests.fixtures.toy_graphs import TWO_CLiques_EDGES


def test_lpa_iteration_line_graph():
    g = Graph.from_undirected_edges([(0, 1), (1, 2)])
    labels = g.init_labels()
    chunk = np.array([g.node_index(1)], dtype=np.int64)
    updates = lpa_iteration_chunk(chunk, g, labels)
    assert updates[0] == 0 or updates[0] == 2


def test_lpa_single_node_stable():
    g = Graph.from_nodes([5])
    labels = g.init_labels()
    chunk = np.array([0], dtype=np.int64)
    assert lpa_iteration_chunk(chunk, g, labels)[0] == 5


def test_lpa_tie_breaks_to_smallest_label():
    g = Graph.from_undirected_edges([(1, 0), (2, 0)])
    labels = g.labels_from_map({0: 0, 1: 1, 2: 2})
    chunk = np.array([g.node_index(0)], dtype=np.int64)
    assert lpa_iteration_chunk(chunk, g, labels)[0] == 1


def test_node_chunks_splits_evenly():
    chunks = node_chunks(10, chunk_divisor=4)
    assert len(chunks) == 4
    assert sum(len(c) for c in chunks) == 10


def test_run_lpa_sequential_two_cliques():
    g = Graph.from_undirected_edges(TWO_CLiques_EDGES)
    res = run_lpa_sequential(g, max_iter=50, seed=1, log_fn=lambda _: None)
    assert res.num_communities >= 2
    assert res.converged


def test_run_lpa_sequential_max_iter_not_converged():
    g = Graph.from_undirected_edges([(0, 1), (1, 2), (2, 0), (2, 3)])
    res = run_lpa_sequential(g, max_iter=1, seed=42, log_fn=lambda _: None)
    assert res.num_levels == 1
    assert res.converged is False


def test_format_iter_log_contains_fields():
    line = format_iter_log(3, 10, 100, 4, 1.5)
    assert "[lpa][iter=3]" in line
    assert "changed=10/100" in line
    assert "workers=4" in line
    assert "Q=" not in line


def test_format_summary_logs_contains_sizes():
    labels = np.array([0, 0, 1, 1, 1], dtype=np.int64)
    lines = format_summary_logs(labels, 5, [0.1, 0.2], True)
    assert any("[lpa][sizes]" in line for line in lines)
    assert any("[lpa][done]" in line for line in lines)


def test_node_chunks_empty_and_single():
    assert node_chunks(0, 4) == []
    chunks = node_chunks(1, 8)
    assert len(chunks) == 1
    assert chunks[0].tolist() == [0]


def test_node_chunks_respects_node_count_cap():
    chunks = node_chunks(3, chunk_divisor=100)
    assert len(chunks) == 3
    assert sum(len(c) for c in chunks) == 3


def test_shuffled_node_chunks_deterministic():
    a = shuffled_node_chunks(12, chunk_divisor=4, seed=7)
    b = shuffled_node_chunks(12, chunk_divisor=4, seed=7)
    assert [c.tolist() for c in a] == [c.tolist() for c in b]
    assert sum(len(c) for c in a) == 12


def test_shuffled_node_chunks_empty():
    assert shuffled_node_chunks(0, 4, seed=1) == []


def test_lpa_votes_only_out_neighbors():
    g = Graph.from_edges([(0, 1), (2, 0)])
    labels = g.init_labels()
    labels[g.node_index(1)] = 42
    chunk = np.array([g.node_index(0)], dtype=np.int64)
    assert lpa_iteration_chunk(chunk, g, labels)[0] == 42


def test_format_iter_log_zero_total():
    line = format_iter_log(1, 0, 0, 1, 0.0)
    assert "changed=0/0" in line
    assert "(0.0%)" in line


def test_format_summary_logs_empty_and_not_converged():
    lines = format_summary_logs(np.array([], dtype=np.int64), 0, [], False)
    assert any("communities=0" in line for line in lines)
    assert any("[lpa][warn]" in line for line in lines)


def test_format_summary_logs_timing_branch():
    lines = format_summary_logs(np.array([0], dtype=np.int64), 1, [], True)
    assert any("iter_min=0.00s" in line for line in lines)


def test_format_runtime_log_and_run_start():
    assert format_runtime_log("ray", "cluster", nodes=10) == "[ray][cluster] nodes=10"
    start = format_run_start("dask", nodes=5, chunks=2, max_iter=50, init_s=0.1, seed=3)
    assert "[dask][start]" in start
    assert "seed=3" in start


def test_lpa_result_labels_dict_from_partition():
    g = Graph.from_edges([(0, 1)])
    res = LpaResult(
        num_communities=1,
        num_levels=1,
        init_time_s=0.0,
        algorithm_time_s=0.1,
        partition_node_ids=g.node_ids,
        partition_labels=np.array([0, 0], dtype=np.int64),
    )
    assert res.labels_dict(g) == {0: 0, 1: 0}
    nodes, labels = res.partition_arrays()
    assert nodes is not None and labels is not None
    assert len(nodes) == 2


def test_run_lpa_sequential_converges_triangle():
    g = Graph.from_undirected_edges([(0, 1), (1, 2), (0, 2)])
    res = run_lpa_sequential(g, max_iter=50, seed=0, log_fn=lambda _: None)
    assert res.converged
    assert res.num_communities >= 1
    assert res.partition_labels is not None


def test_lpa_deterministic_with_seed():
    g = Graph.from_undirected_edges(TWO_CLiques_EDGES)
    r1 = run_lpa_sequential(g, max_iter=20, seed=42, log_fn=lambda _: None)
    g2 = Graph.from_undirected_edges(TWO_CLiques_EDGES)
    r2 = run_lpa_sequential(g2, max_iter=20, seed=42, log_fn=lambda _: None)
    assert r1.num_communities == r2.num_communities
