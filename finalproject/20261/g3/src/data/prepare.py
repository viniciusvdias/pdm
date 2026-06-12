"""CLI utilitário: valida/normaliza o PaySim e reporta estatísticas.

A lógica de normalização vive em ``common/schema.py`` (reutilizada por producer e
baseline). Este script serve para inspecionar rapidamente um CSV (sample ou
completo) e confirmar que o parsing está coerente antes de subir a stack.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter

sys.path.insert(0, "/app/src")
try:
    from common.schema import iter_transactions, open_text
except ModuleNotFoundError:  # execução local fora do container
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from common.schema import iter_transactions, open_text


def main() -> int:
    ap = argparse.ArgumentParser(description="Valida/normaliza PaySim")
    ap.add_argument("--input", required=True)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    types = Counter()
    n = 0
    frauds = 0
    min_t = None
    max_t = None
    with open_text(args.input) as f:
        for tx in iter_transactions(f):
            types[tx.type] += 1
            frauds += tx.is_fraud
            min_t = tx.event_time if min_t is None else min(min_t, tx.event_time)
            max_t = tx.event_time if max_t is None else max(max_t, tx.event_time)
            n += 1
            if args.limit and n >= args.limit:
                break

    print(f"Transações válidas: {n}")
    print(f"Fraudes (isFraud=1): {frauds}")
    print(f"Distribuição de tipos: {dict(types)}")
    print(f"event_time range (ms): {min_t} .. {max_t}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
