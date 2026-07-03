"""Integration: precomputed Orkut 0.1% fixture loads instantly."""

from __future__ import annotations

import pytest

from preprocessing.graph_artifact import load_graph_artifact
from tests.integration.dataset import (
    FIXTURE_EDGE_COUNT,
    FIXTURE_NODE_COUNT,
    FIXTURE_PATH,
)


@pytest.mark.integration
def test_load_orkut_fixture_fast():
    loaded = load_graph_artifact(FIXTURE_PATH)
    assert loaded.node_count == FIXTURE_NODE_COUNT
    assert loaded.edge_count == FIXTURE_EDGE_COUNT
    assert loaded.load_time_s < 1.0
    assert loaded.graph.num_nodes == FIXTURE_NODE_COUNT
    assert int(loaded.graph.m) == FIXTURE_EDGE_COUNT

    print(
        f"[fixture] {loaded.node_count:,} nodes, {loaded.edge_count:,} stored arcs "
        f"in {loaded.load_time_s * 1000:.1f} ms"
    )
