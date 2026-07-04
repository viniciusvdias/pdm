#!/bin/bash

# Raíz;
cd "$(dirname "$0")/.." || exit

echo "--- -------------------- ---"
echo "Shutting down the project..."
echo "--- -------------------- ---"

docker compose down

echo "Stop script finished!"