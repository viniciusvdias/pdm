"""Gera uma amostra pequena (<=1MB) do PaySim para o `datasample/`.

Estratégia (determinística):
- Mantém um bloco CONTÍGUO das primeiras ``--head`` linhas. Um bloco contíguo
  preserva os relacionamentos locais entre contas (arestas origem->destino), o
  que permite que o settlement e parte dos padrões AML funcionem no quick start.
- Varre até ``--scan`` linhas e ANEXA quaisquer linhas com ``isFraud=1`` ainda
  não incluídas, para exercitar a avaliação de precision/recall do AML.
- Escreve gzip e garante o limite de tamanho (aborta se exceder ``--max-bytes``).

Sem dependências externas: roda em qualquer ``python:3.11-slim``.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import sys

HEADER = (
    "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,"
    "nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n"
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Gera amostra <=1MB do PaySim")
    ap.add_argument("--input", required=True, help="CSV PaySim completo")
    ap.add_argument("--output", required=True, help="saida .csv.gz")
    ap.add_argument("--head", type=int, default=12000,
                    help="linhas contíguas iniciais (preserva grafo local)")
    ap.add_argument("--scan", type=int, default=2_000_000,
                    help="linhas varridas em busca de fraudes")
    ap.add_argument("--max-bytes", type=int, default=1_000_000,
                    help="tamanho máximo do arquivo de saída (bytes)")
    args = ap.parse_args()

    included_fraud = 0
    with open(args.input, "r", newline="", encoding="utf-8") as fin, \
            gzip.open(args.output, "wt", newline="", encoding="utf-8") as fout:
        reader = csv.reader(fin)
        header = next(reader)
        fout.write(",".join(header) + "\n")

        kept = 0
        seen = 0
        for row in reader:
            if seen < args.head:
                fout.write(",".join(row) + "\n")
                kept += 1
            elif seen < args.scan:
                # PaySim: isFraud é a penúltima coluna.
                if len(row) >= 10 and row[-2] == "1":
                    fout.write(",".join(row) + "\n")
                    kept += 1
                    included_fraud += 1
            else:
                break
            seen += 1

    import os
    size = os.path.getsize(args.output)
    print(f"Amostra: {kept} linhas ({included_fraud} fraudes extras) -> "
          f"{size/1024:.0f} KB em {args.output}")
    if size > args.max_bytes:
        print(f"ERRO: {size} bytes > limite {args.max_bytes}. "
              f"Reduza --head.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
