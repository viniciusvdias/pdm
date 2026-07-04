"""Sample nodes, induced subgraph, and largest connected component."""

from __future__ import annotations

import random
from collections import defaultdict, deque
from collections.abc import Callable
from pathlib import Path

import networkx as nx

from preprocessing.load_snap import collect_node_set, is_large_raw, iter_snap_edges


def sample_connected_node_ids(
    edges: list[tuple[int, int]],
    lcc_nodes: list[int] | set[int],
    fraction_pct: float,
    seed: int,
) -> set[int]:
    """
    BFS expansion from a random LCC seed until k nodes are collected.

    Unlike uniform random sampling + induced subgraph + LCC (which collapses to
    a handful of nodes), this yields a connected ~fraction_pct subgraph.
    """
    if fraction_pct >= 100:
        return set(lcc_nodes)

    rng = random.Random(seed)
    node_pool = list(lcc_nodes)
    k = max(1, int(len(node_pool) * fraction_pct / 100))
    lcc_set = set(lcc_nodes)
    adj: dict[int, list[int]] = defaultdict(list)
    for s, d in edges:
        if s in lcc_set and d in lcc_set:
            adj[s].append(d)
            adj[d].append(s)

    start = rng.choice(node_pool)
    visited: set[int] = {start}
    queue: deque[int] = deque([start])
    while len(visited) < k and queue:
        u = queue.popleft()
        neighbors = adj.get(u, [])
        rng.shuffle(neighbors)
        for v in neighbors:
            if v not in visited:
                visited.add(v)
                queue.append(v)
                if len(visited) >= k:
                    break
    return visited


def sample_connected_node_ids_streaming(
    path: Path,
    lcc_nodes: list[int] | set[int],
    fraction_pct: float,
    seed: int,
    *,
    log_fn: Callable[[str], None] | None = print,
) -> set[int]:
    """
    BFS connected sample by streaming an undirected SNAP file (memory-safe).

    Each BFS layer scans the full edge list once (large soc-Pokec files).
    """
    if fraction_pct >= 100:
        return set(lcc_nodes)

    rng = random.Random(seed)
    node_pool = list(lcc_nodes)
    k = max(1, int(len(node_pool) * fraction_pct / 100))
    lcc_set = set(lcc_nodes)
    start = rng.choice(node_pool)
    visited: set[int] = {start}
    frontier: set[int] = {start}
    layer = 0

    while len(visited) < k and frontier:
        layer += 1
        if log_fn is not None:
            log_fn(
                f"[load] BFS layer {layer}: frontier={len(frontier)} "
                f"visited={len(visited)}/{k} — scanning {path.name} ...",
                flush=True,
            )
        next_frontier: set[int] = set()
        for u, v in iter_snap_edges(path):
            if u in frontier and v in lcc_set and v not in visited:
                next_frontier.add(v)
            elif v in frontier and u in lcc_set and u not in visited:
                next_frontier.add(u)
        if not next_frontier:
            break
        candidates = list(next_frontier)
        rng.shuffle(candidates)
        newly_added: list[int] = []
        for node in candidates:
            if len(visited) >= k:
                break
            visited.add(node)
            newly_added.append(node)
        frontier = set(newly_added)

    return visited


def collect_lcc_node_ids(raw_path: Path) -> list[int] | set[int]:
    """
    Node ids in the largest connected component of the full SNAP graph.

    For very large raw files, skip in-memory NetworkX LCC and return all node
    ids from a streaming scan (soc-Pokec 100% uses this path).
    """
    if is_large_raw(raw_path):
        return collect_node_set(raw_path)

    edges = list(iter_snap_edges(raw_path))
    lcc_edges, _ = extract_lcc(edges)
    nodes = {s for s, _ in lcc_edges} | {d for _, d in lcc_edges}
    return sorted(nodes)


def induced_edges(
    edges: list[tuple[int, int]],
    nodes: set[int],
) -> list[tuple[int, int]]:
    return [(s, d) for s, d in edges if s in nodes and d in nodes]


def extract_lcc(
    edges: list[tuple[int, int]],
) -> tuple[list[tuple[int, int]], int]:
    """
    Return undirected edges inside the largest connected component.
    """
    if not edges:
        return [], 0

    g = nx.Graph()
    for s, d in edges:
        if s != d:
            g.add_edge(s, d)

    if g.number_of_nodes() == 0:
        return [], 0

    components = list(nx.connected_components(g))
    lcc_nodes = max(components, key=len)
    dropped = g.number_of_nodes() - len(lcc_nodes)

    lcc_edges = [(s, d) for s, d in edges if s in lcc_nodes and d in lcc_nodes]
    return lcc_edges, dropped
