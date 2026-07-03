#!/usr/bin/env bash
# Load Orkut fraction and print measured RSS + CSR estimate.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

FRACTION="${1:-0.1}"
RAW="${GRAPH_RAW_PATH:-data/raw/soc-orkut-relationships.txt}"

python - <<PY
from pathlib import Path
from benchmark.memory_estimate import (
    POKEC_100_EDGE_COUNT,
    POKEC_100_NODE_COUNT,
    estimate_csr_bytes,
    estimate_for_fraction,
    format_estimate_report,
)
from benchmark.metrics import track_memory_peaks
from preprocessing.load_graph import load_graph_from_snap

raw = Path("${RAW}")
fraction = float("${FRACTION}")
if not raw.is_file():
    raise SystemExit(f"Missing {raw} — run: bash bin/download_dataset.sh")

est = estimate_for_fraction(100, POKEC_100_NODE_COUNT, POKEC_100_EDGE_COUNT)
print(format_estimate_report(est))
print()

with track_memory_peaks() as peaks:
    loaded = load_graph_from_snap(raw, fraction_pct=fraction, seed=42)

csr = estimate_csr_bytes(loaded.node_count, loaded.edge_count)
print(
    f"Loaded {fraction}%: {loaded.node_count:,} nodes, "
    f"{loaded.edge_count:,} stored arcs in {loaded.load_time_s:.1f}s"
)
print(f"  driver RSS peak: {peaks.driver_rss_mb:.0f} MB")
print(f"  process tree RSS peak: {peaks.process_tree_rss_mb:.0f} MB")
print(f"  CSR steady (estimate): {csr.csr_steady_mb:.1f} MB")
PY
