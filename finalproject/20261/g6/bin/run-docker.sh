#!/usr/bin/env bash
# Build and run full benchmark on this machine (Docker).
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up --build "$@"
