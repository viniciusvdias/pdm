"""Edge case tests."""

from graph.graph import Graph
from lpa_core.lpa import run_lpa_sequential


def test_empty_graph():
    g = Graph.from_nodes([])
    res = run_lpa_sequential(g, log_fn=lambda _: None)
    assert res.num_communities == 0
    assert res.labels == {}
    assert res.node_count == 0
    assert res.converged


def test_single_node_lpa_stable():
    g = Graph.from_nodes([1])
    labels = g.init_labels()
    assert labels[0] == 1
    res = run_lpa_sequential(g, log_fn=lambda _: None)
    assert res.converged
    assert res.num_communities == 1
