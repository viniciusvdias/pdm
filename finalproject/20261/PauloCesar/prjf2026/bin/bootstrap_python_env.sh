#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="$(basename "$PROJECT_ROOT")"
VENV_DIR="$PROJECT_ROOT/venv_incendios"
REQUIREMENTS_FILE="$PROJECT_ROOT/misc/requirements.txt"
STAMP_FILE="$VENV_DIR/.requirements_installed"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 nao encontrado no PATH. Instale o Python 3 para usar os scripts de $PROJECT_NAME." >&2
  exit 1
fi

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Criando ambiente Python local em $VENV_DIR..." >&2
  python3 -m venv "$VENV_DIR" >&2
fi

PYTHON_BIN="$VENV_DIR/bin/python3"
PIP_BIN="$VENV_DIR/bin/pip"

if [[ ! -f "$STAMP_FILE" || "$REQUIREMENTS_FILE" -nt "$STAMP_FILE" ]]; then
  echo "Instalando dependencias Python locais de $PROJECT_NAME..." >&2
  "$PYTHON_BIN" -m pip install --upgrade pip >&2
  "$PIP_BIN" install -r "$REQUIREMENTS_FILE" >&2
  touch "$STAMP_FILE"
fi

echo "$VENV_DIR"
