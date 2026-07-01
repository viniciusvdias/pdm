#!/usr/bin/env python3
"""Build the small connected sample committed under ``datasample/``.

The full RMSP road graph (770k vertices / 2.25M directed edges, derived from the
public Geofabrik extract) is far too large to keep in the repository. This
script extracts a small **connected** subgraph via breadth-first search so that
the shortest-path workloads still reach many vertices, then writes the same
Parquet contract consumed by the experiment:

- ``vertices.parquet`` -> columns: id, osm_id, lat, lon
- ``edges.parquet``    -> columns: src, dst, length_m, travel_time_s

Node ids are remapped to a contiguous 0..N-1 range so the sample is
self-contained. The result is intended to stay under 1 MB.

Usage (run against a local copy of the full graph):

    python bin/make_datasample.py \
        --full-dir /path/to/full/rmsp_graph \
        --out-dir datasample \
        --max-vertices 3500
"""

from __future__ import annotations

import argparse
from collections import deque
from pathlib import Path

import pandas as pd


def build_sample(full_dir: Path, max_vertices: int, seed_id: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    vertices = pd.read_parquet(full_dir / "vertices.parquet")
    edges = pd.read_parquet(full_dir / "edges.parquet")

    # Undirected adjacency for BFS so the sampled region stays connected.
    adjacency: dict[int, list[int]] = {}
    for src, dst in zip(edges["src"].to_numpy(), edges["dst"].to_numpy()):
        adjacency.setdefault(int(src), []).append(int(dst))
        adjacency.setdefault(int(dst), []).append(int(src))

    visited: list[int] = []
    seen: set[int] = {seed_id}
    queue: deque[int] = deque([seed_id])
    while queue and len(visited) < max_vertices:
        node = queue.popleft()
        visited.append(node)
        for nxt in adjacency.get(node, ()):  # deterministic: adjacency preserves file order
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)

    keep = set(visited)
    remap = {old: new for new, old in enumerate(visited)}

    sub_vertices = (
        vertices[vertices["id"].isin(keep)]
        .assign(id=lambda df: df["id"].map(remap))
        .sort_values("id")
        .reset_index(drop=True)
    )
    sub_edges = (
        edges[edges["src"].isin(keep) & edges["dst"].isin(keep)]
        .assign(src=lambda df: df["src"].map(remap), dst=lambda df: df["dst"].map(remap))
        .reset_index(drop=True)
    )
    return sub_vertices, sub_edges


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-dir", required=True, help="Directory with the full vertices/edges parquet")
    parser.add_argument("--out-dir", default="datasample")
    parser.add_argument("--max-vertices", type=int, default=3500)
    parser.add_argument("--seed-id", type=int, default=0, help="Vertex id used as BFS seed")
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    vtx, edg = build_sample(Path(args.full_dir), args.max_vertices, args.seed_id)
    vtx.to_parquet(out / "vertices.parquet", index=False)
    edg.to_parquet(out / "edges.parquet", index=False)

    v_bytes = (out / "vertices.parquet").stat().st_size
    e_bytes = (out / "edges.parquet").stat().st_size
    print(f"vertices: {len(vtx):>7} rows  {v_bytes/1024:8.1f} KiB")
    print(f"edges:    {len(edg):>7} rows  {e_bytes/1024:8.1f} KiB")
    print(f"total:                    {(v_bytes + e_bytes)/1024:8.1f} KiB")


if __name__ == "__main__":
    main()
