#!/bin/bash
set -e

echo "➡️  Parando e removendo o ambiente Compose..."

docker compose down --remove-orphans

echo "✅  Ambiente Compose finalizado."