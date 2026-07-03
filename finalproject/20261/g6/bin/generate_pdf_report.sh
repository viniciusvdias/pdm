#!/usr/bin/env bash
# Build consolidated PDF report from reports/REPORT_COMPLETO.md
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MD="reports/REPORT_COMPLETO.md"
PDF="reports/REPORT_COMPLETO.pdf"

if [[ ! -f "$MD" ]]; then
  echo "Missing $MD" >&2
  exit 1
fi

pandoc "$MD" -o "$PDF" \
  --resource-path=".:reports:results:docs" \
  --pdf-engine=xelatex \
  -V geometry:margin=2.5cm \
  -V lang=pt-BR \
  -V fontsize=11pt

echo "Wrote $PDF"
