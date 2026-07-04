"""Initial labels for LPA."""

import numpy as np

from graph.graph import Graph


def test_init_labels_returns_own_node_ids():
    g = Graph.from_undirected_edges([(0, 1), (1, 2), (0, 2)])
    labels = g.init_labels()
    assert np.array_equal(labels, g.node_ids.astype(np.int64))
