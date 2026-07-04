"""Load SNAP edge lists (directed or undirected)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import numpy as np

from graph.graph import Graph, symmetrize_coo

LARGE_RAW_BYTES = 50 * 1024 * 1024
_READ_BUFFER = 8 * 1024 * 1024


def is_large_raw(path: Path) -> bool:
    return path.is_file() and path.stat().st_size >= LARGE_RAW_BYTES


def iter_snap_edges(path: Path) -> Iterator[tuple[int, int]]:
    """Yield one (u, v) pair per non-comment line (as stored in the SNAP file)."""
    with path.open("rb", buffering=_READ_BUFFER) as f:
        for line in f:
            if not line or line[0:1] == b"#":
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                u = int(parts[0])
                v = int(parts[1])
            except ValueError:
                continue
            if u != v:
                yield u, v


def iter_directed_edges(path: Path) -> Iterator[tuple[int, int]]:
    """Alias for :func:`iter_snap_edges` (one stored arc per line)."""
    yield from iter_snap_edges(path)


def _read_edges_filtered(
    path: Path,
    nodes: set[int] | None = None,
    *,
    directed: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    u_list: list[int] = []
    v_list: list[int] = []
    for a, b in iter_snap_edges(path):
        if nodes is None or (a in nodes and b in nodes):
            u_list.append(a)
            v_list.append(b)
    if not u_list:
        empty = np.empty(0, dtype=np.int32)
        return empty, empty
    src = np.asarray(u_list, dtype=np.int32)
    dst = np.asarray(v_list, dtype=np.int32)
    if not directed:
        src, dst = symmetrize_coo(src, dst)
    return src, dst


def read_edges_coo(path: Path, *, directed: bool = False) -> tuple[np.ndarray, np.ndarray]:
    """Read SNAP edge list into COO (single pass; symmetrize when undirected)."""
    return _read_edges_filtered(path, directed=directed)


def read_edges_coo_subset(
    path: Path,
    nodes: set[int],
    *,
    directed: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    """Stream edges whose endpoints lie in ``nodes``."""
    if not nodes:
        empty = np.empty(0, dtype=np.int32)
        return empty, empty
    return _read_edges_filtered(path, nodes, directed=directed)


def graph_from_snap_file(path: Path, *, directed: bool = False) -> Graph:
    """TXT → numpy COO → out-CSR."""
    src, dst = read_edges_coo(path, directed=directed)
    return Graph.from_coo(src, dst)


def collect_node_set(path: Path) -> set[int]:
    nodes: set[int] = set()
    for u, v in iter_snap_edges(path):
        nodes.add(u)
        nodes.add(v)
    return nodes
