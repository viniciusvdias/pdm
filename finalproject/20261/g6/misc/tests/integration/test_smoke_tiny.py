"""Smoke test with tiny in-memory graph."""

from graph.graph import Graph
from lpa_core.lpa import run_lpa_sequential


def test_sequential_lpa_on_fixture(tiny_graph: Graph):
    res = run_lpa_sequential(tiny_graph, log_fn=lambda _: None)
    assert res.num_communities >= 1
