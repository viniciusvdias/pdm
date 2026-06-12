"""Amplificador determinístico do PaySim para atingir o piso de "big data".

O PaySim tem ~470 MB (< 1 GB). Replicamos o dataset ``--factor`` vezes criando
*shards* com contas em namespaces distintos:

- Shard 0 reproduz os IDs originais.
- Shard s>0 sufixa cada conta com ``#s`` (ex.: ``C1231006815#3``).

Isso multiplica volume e número de contas PRESERVANDO:
- a distribuição estatística de ``type``/``amount`` (linhas são cópias exatas),
- as arestas origem->destino DENTRO de cada shard, de modo que padrões como o
  ciclo A->B->C->A continuam existindo em cada cópia.

Fator 3 (default) ~ 1,4 GB; fator 10 ~ 4,7 GB. Saída em CSV (mesmo schema).
Sem dependências externas.
"""

from __future__ import annotations

import argparse
import csv
import sys


def _suffix_account(name: str, shard: int) -> str:
    if shard == 0 or not name:
        return name
    return f"{name}#{shard}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Amplifica o PaySim ×factor")
    ap.add_argument("--input", required=True, help="CSV PaySim completo")
    ap.add_argument("--output", required=True, help="CSV amplificado de saída")
    ap.add_argument("--factor", type=int, default=3,
                    help="quantas cópias (>=1; 3 ~ 1,4GB)")
    ap.add_argument("--limit", type=int, default=0,
                    help="limita linhas lidas do input (0 = todas; p/ testes)")
    args = ap.parse_args()

    if args.factor < 1:
        print("ERRO: --factor deve ser >= 1", file=sys.stderr)
        return 1

    written = 0
    with open(args.output, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        for shard in range(args.factor):
            with open(args.input, "r", newline="", encoding="utf-8") as fin:
                reader = csv.reader(fin)
                header = next(reader)
                if shard == 0:
                    writer.writerow(header)
                for i, row in enumerate(reader):
                    if args.limit and i >= args.limit:
                        break
                    if len(row) >= 7:
                        row[3] = _suffix_account(row[3], shard)   # nameOrig
                        row[6] = _suffix_account(row[6], shard)   # nameDest
                    writer.writerow(row)
                    written += 1
            print(f"  shard {shard} concluído ({written} linhas acumuladas)")

    print(f"Amplificado: {written} linhas (×{args.factor}) -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
