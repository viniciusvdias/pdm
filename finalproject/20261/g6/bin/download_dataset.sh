#!/usr/bin/env bash
# Download SNAP soc-Orkut undirected graph into data/raw/
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_DIR="data/raw"
OUT="$RAW_DIR/soc-orkut-relationships.txt"
URL="https://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz"
GZ="$RAW_DIR/com-orkut.ungraph.txt.gz"

mkdir -p "$RAW_DIR"
if [[ -f "$OUT" ]]; then
  echo "Already present: $OUT"
  exit 0
fi

echo "Downloading $URL ..."
curl -fsSL "$URL" -o "$GZ"
gunzip -f "$GZ"
mv -f "$RAW_DIR/com-orkut.ungraph.txt" "$OUT"
echo "Wrote $OUT"
