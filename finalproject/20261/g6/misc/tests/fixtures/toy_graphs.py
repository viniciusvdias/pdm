"""Small deterministic graphs for unit tests."""

from __future__ import annotations


def _bidirected_clique(nodes: list[int]) -> list[tuple[int, int]]:
    return [(u, v) for u in nodes for v in nodes if u != v]


TRIANGLE_EDGES = [(0, 1), (1, 2), (0, 2)]

# Two 3-node cliques as directed pairs (test helper expands via from_undirected_edges)
TWO_CLiques_EDGES = [(0, 1), (1, 2), (0, 2), (3, 4), (4, 5), (3, 5)]

DIRECTED_SAMPLE_LINES = [
    "# comment\n",
    "1 2\n",
    "3 3\n",
    "3 4\n",
]
