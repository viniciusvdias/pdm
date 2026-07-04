"""Load SNAP graphs directly into CSR (in-memory)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from graph.graph import Graph
from preprocessing.load_snap import (
    graph_from_snap_file,
    is_large_raw,
    iter_snap_edges,
    read_edges_coo_subset,
)
from preprocessing.sample_lcc import (
    collect_lcc_node_ids,
    extract_lcc,
    induced_edges,
    sample_connected_node_ids,
    sample_connected_node_ids_streaming,
)


@dataclass(frozen=True)
class GraphLoadResult:
    graph: Graph
    load_time_s: float
    node_count: int
    edge_count: int  # stored directed arcs (``graph.m``)
    fraction_pct: float


def load_graph_from_snap(
    path: Path,
    *,
    fraction_pct: float = 100.0,
    seed: int = 42,
    directed: bool = False,
) -> GraphLoadResult:
    """Read a SNAP edge list and build an in-memory out-CSR graph."""
    path = Path(path)
    t0 = time.perf_counter()

    if not is_large_raw(path):
        lcc_nodes = collect_lcc_node_ids(path)
        all_edges = list(iter_snap_edges(path))
        if fraction_pct >= 100:
            sampled = set(lcc_nodes)
        else:
            sampled = sample_connected_node_ids(
                all_edges, lcc_nodes, fraction_pct, seed
            )
        induced = induced_edges(all_edges, sampled)
        lcc_edges, _dropped = extract_lcc(induced)
        if directed:
            graph = Graph.from_edges(lcc_edges)
        else:
            graph = Graph.from_undirected_edges(lcc_edges)
    elif fraction_pct >= 100:
        graph = graph_from_snap_file(path, directed=directed)
    else:
        print(
            f"[load] collecting node ids from {path.name} (1 pass over edge list) ...",
            flush=True,
        )
        lcc_nodes = collect_lcc_node_ids(path)
        print(f"[load] {len(lcc_nodes):,} nodes — BFS sample {fraction_pct}% ...", flush=True)
        sampled = sample_connected_node_ids_streaming(
            path, lcc_nodes, fraction_pct, seed
        )
        print(
            f"[load] sampled {len(sampled):,} nodes — extracting induced edges ...",
            flush=True,
        )
        src, dst = read_edges_coo_subset(path, sampled, directed=directed)
        graph = (
            Graph.from_coo(src, dst) if src.size > 0 else Graph.from_nodes([])
        )

    elapsed = time.perf_counter() - t0
    return GraphLoadResult(
        graph=graph,
        load_time_s=elapsed,
        node_count=graph.num_nodes,
        edge_count=int(graph.m),
        fraction_pct=fraction_pct,
    )
