"""Focused tests for Graph CSR construction and helpers (mutation targets)."""

import numpy as np
import pytest

from graph.graph import Graph


def test_from_nodes_empty():
    g = Graph.from_nodes([])
    assert g.num_nodes == 0
    assert g.m == 0.0
    assert g.edges() == []


def test_contains_and_node_index():
    g = Graph.from_edges([(10, 20), (20, 30)])
    assert 10 in g
    assert 99 not in g
    assert g.node_index(20) == g.node_index(20)
    with pytest.raises(KeyError):
        g.node_index(99)


def test_out_degree_of_missing_node_is_zero():
    g = Graph.from_edges([(1, 2)])
    assert g.out_degree_of(99) == 0.0
    assert g.degree_of(1) == 1.0
    assert g.out_degrees[0] == 1.0


def test_labels_round_trip():
    g = Graph.from_edges([(0, 1), (1, 2)])
    labels = g.labels_from_map({0: 7, 1: 7, 2: 9})
    assert labels[g.node_index(0)] == 7
    assert labels[g.node_index(2)] == 9
    back = g.labels_to_dict(labels)
    assert back[0] == 7 and back[2] == 9


def test_from_coo_already_deduped_skips_dedupe():
    src = np.array([0, 1], dtype=np.int32)
    dst = np.array([1, 2], dtype=np.int32)
    g = Graph.from_coo(src, dst, already_deduped=True)
    assert g.m == 2.0
    assert sorted(g.edges()) == [(0, 1), (1, 2)]


def test_from_coo_deduplicates_parallel_arcs():
    src = np.array([0, 0, 0], dtype=np.int32)
    dst = np.array([1, 1, 1], dtype=np.int32)
    g = Graph.from_coo(src, dst)
    assert g.m == 1.0


def test_from_csr_arrays_round_trip():
    node_ids = np.array([5, 10], dtype=np.int32)
    indptr = np.array([0, 1, 1], dtype=np.int32)
    neighbors = np.array([1], dtype=np.int32)
    g = Graph.from_csr_arrays(node_ids, indptr, neighbors, m=1.0)
    assert g.num_nodes == 2
    assert g.neighbor_indices(5).tolist() == [1]
    assert g.neighbor_indices(10).size == 0


def test_directed_only_out_neighbors_vote():
    """LPA semantics: node 0 sees only outgoing target, not reverse arc."""
    g = Graph.from_edges([(0, 1), (2, 0)])
    idx0 = g.node_index(0)
    labels = g.init_labels()
    from lpa_core.lpa import lpa_iteration_chunk

    chunk = np.array([idx0], dtype=np.int64)
    # out-neighbor of 0 is 1 only; label at 1 wins if we force it
    labels[g.node_index(1)] = 99
    update = lpa_iteration_chunk(chunk, g, labels)
    assert update[0] == 99


def test_from_edges_empty_and_all_self_loops():
    empty = Graph.from_edges([])
    assert empty.num_nodes == 0
    only_loops = Graph.from_edges([(1, 1), (2, 2)])
    assert only_loops.num_nodes == 0


def test_multi_out_edges_sorted_in_csr():
    g = Graph.from_edges([(0, 2), (0, 1), (0, 3)])
    idx0 = g.node_index(0)
    nbrs = [int(g.node_ids[i]) for i in g.neighbor_indices(0)]
    assert nbrs == [1, 2, 3]
    assert g.out_degrees[idx0] == 3.0
