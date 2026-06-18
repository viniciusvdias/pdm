#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

echo "Parando e removendo containers..."
docker compose down -v
echo "Pronto."
