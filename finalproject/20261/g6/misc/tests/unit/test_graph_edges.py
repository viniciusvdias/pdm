"""Tests for Graph helper methods."""

import numpy as np

from graph.graph import Graph


def test_graph_keeps_directed_edges():
    g = Graph.from_edges([(0, 1)])
    assert g.edges() == [(0, 1)]
    assert (1, 0) not in g.edges()


def test_graph_ignores_self_loops_in_from_edges():
    g = Graph.from_edges([(1, 1), (1, 2)])
    assert 1 in g and 2 in g
    assert g.edges() == [(1, 2)]


def test_graph_m_edge_count():
    g = Graph.from_edges([(0, 1), (1, 2)])
    assert g.m == 2.0


def test_from_coo_deduplicates_duplicate_arcs():
    src = np.array([0, 0, 1, 1], dtype=np.int32)
    dst = np.array([1, 1, 2, 2], dtype=np.int32)
    g = Graph.from_coo(src, dst)
    assert g.m == 2.0
    assert g.out_degree_of(0) == 1.0
    assert g.out_degree_of(1) == 1.0


def test_neighbor_indices_follow_outgoing_edges():
    g = Graph.from_edges([(10, 20), (20, 30)])
    idx20 = g.node_index(20)
    idx30 = g.node_index(30)
    assert set(g.neighbor_indices(10)) == {idx20}
    assert set(g.neighbor_indices(20)) == {idx30}
    assert g.neighbor_indices(30).size == 0


def test_from_undirected_edges_expands_both_ways():
    g = Graph.from_undirected_edges([(1, 2)])
    assert g.m == 2.0
    assert set(g.neighbor_indices(2)) == {g.node_index(1)}
    assert set(g.neighbor_indices(1)) == {g.node_index(2)}


def test_iter_directed_edges_matches_edges():
    g = Graph.from_edges([(0, 1), (1, 2), (2, 0)])
    assert sorted(g.iter_directed_edges()) == sorted(g.edges())
