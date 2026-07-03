#!/usr/bin/env bash
# Build tests/integration/fixtures/orkut_0p1pct.npz (synthetic undirected sample for CI).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

RAW="${GRAPH_RAW_PATH:-data/raw/soc-orkut-relationships.txt}"
OUT="tests/integration/fixtures/orkut_0p1pct"
FRACTION="${INTEGRATION_FRACTION:-0.1}"
SEED="${INTEGRATION_SEED:-42}"

python - <<PY
from pathlib import Path

import networkx as nx

from graph.graph import Graph
from preprocessing.graph_artifact import save_graph_artifact
from preprocessing.load_graph import load_graph_from_snap

raw = Path("${RAW}")
out = Path("${OUT}")
fraction = float("${FRACTION}")
seed = int("${SEED}")

if raw.is_file():
    print(f"Building {out}.npz from {raw} ({fraction}% seed={seed}) ...")
    loaded = load_graph_from_snap(raw, fraction_pct=fraction, seed=seed, directed=False)
    graph = loaded.graph
    node_count = loaded.node_count
    edge_count = loaded.edge_count
    load_time_s = loaded.load_time_s
    source = str(raw)
else:
    print("Raw Orkut missing — writing synthetic Barabási–Albert fixture ...")
    g = nx.barabasi_albert_graph(1632, 3, seed=seed)
    edges = [(int(u), int(v)) for u, v in g.edges()]
    graph = Graph.from_undirected_edges(edges)
    node_count = graph.num_nodes
    edge_count = int(graph.m)
    load_time_s = 0.0
    source = "synthetic:barabasi_albert(n=1632,m=3)"

meta = {
    "dataset_slug": "orkut",
    "fraction_pct": fraction,
    "seed": seed,
    "node_count": node_count,
    "edge_count": edge_count,
    "source": source,
    "load_time_s_build": load_time_s,
    "graph_directed": False,
}
npz, meta_path = save_graph_artifact(graph, out, meta=meta)
print(f"Wrote {npz} ({npz.stat().st_size / 1024:.0f} KB)")
print(f"Wrote {meta_path}")
print(f"  nodes={node_count:,} edges={edge_count:,}")
PY
