"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import pytest

from graph.graph import Graph
from tests.fixtures.toy_graphs import TWO_CLiques_EDGES


@pytest.fixture
def tiny_graph() -> Graph:
    """Small graph (~6 nodes) for fast unit/integration smoke."""
    return Graph.from_undirected_edges(TWO_CLiques_EDGES)
