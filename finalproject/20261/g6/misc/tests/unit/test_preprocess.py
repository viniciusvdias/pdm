"""Tests for SNAP graph loading."""

from pathlib import Path

from preprocessing.load_graph import load_graph_from_snap
from preprocessing.load_snap import iter_directed_edges, read_edges_coo
from preprocessing.sample_lcc import (
    extract_lcc,
    induced_edges,
    sample_connected_node_ids,
)
from tests.fixtures.toy_graphs import DIRECTED_SAMPLE_LINES


def test_iter_directed_edges_one_entry_per_line(tmp_path: Path):
    p = tmp_path / "edges.txt"
    p.write_text("".join(DIRECTED_SAMPLE_LINES))
    edges = list(iter_directed_edges(p))
    assert (1, 2) in edges
    assert (3, 4) in edges
    assert all(s != d for s, d in edges)


def test_sample_connected_reproducible():
    edges = [(0, 1), (1, 2), (0, 2), (3, 4)]
    nodes = sorted({s for s, _ in edges} | {d for _, d in edges})
    a = sample_connected_node_ids(edges, nodes, 50, 42)
    b = sample_connected_node_ids(edges, nodes, 50, 42)
    assert a == b
    assert len(a) >= 1


def test_sample_connected_preserves_fraction():
    edges = [(i, i + 1) for i in range(99)]
    nodes = list(range(100))
    sampled = sample_connected_node_ids(edges, nodes, 10, 42)
    assert len(sampled) == 10
    induced = induced_edges(edges, sampled)
    lcc, _ = extract_lcc(induced)
    assert len({s for s, _ in lcc} | {d for _, d in lcc}) == 10


def test_load_graph_from_snap_small(tmp_path: Path):
    raw = tmp_path / "raw.txt"
    raw.write_text("1 2\n2 3\n3 1\n4 5\n5 4\n")
    result = load_graph_from_snap(raw, fraction_pct=100, seed=42)
    assert result.node_count >= 3
    assert result.edge_count >= 3
    assert result.load_time_s >= 0.0


def test_read_edges_coo_keeps_directed_arcs(tmp_path: Path):
    raw = tmp_path / "raw.txt"
    raw.write_text("1 2\n2 3\n3 1\n")
    assert len(list(iter_directed_edges(raw))) == 3
    src, dst = read_edges_coo(raw, directed=True)
    assert len(src) == 3
    assert list(zip(src.tolist(), dst.tolist())) == [(1, 2), (2, 3), (3, 1)]


def test_read_edges_coo_symmetrizes_undirected(tmp_path: Path):
    raw = tmp_path / "raw.txt"
    raw.write_text("1 2\n2 3\n")
    src, dst = read_edges_coo(raw, directed=False)
    pairs = sorted(zip(src.tolist(), dst.tolist()))
    assert pairs == [(1, 2), (2, 1), (2, 3), (3, 2)]
