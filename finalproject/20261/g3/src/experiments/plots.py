"""Gera gráficos (com barras de erro) a partir dos CSVs de resultados.

Lê um CSV no formato agregado ``config,metric,mean,std,runs`` (produzido por
``aggregate``) e emite um PNG de barras por métrica. Usado pelos experimentos
para ilustrar média ± desvio padrão conforme exigido pelo template do relatório.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> int:
    ap = argparse.ArgumentParser(description="Plota resultados agregados")
    ap.add_argument("--input", required=True,
                    help="CSV: config,metric,mean,std,runs")
    ap.add_argument("--metric", required=True, help="métrica a plotar")
    ap.add_argument("--output", required=True, help="PNG de saída")
    ap.add_argument("--title", default="")
    ap.add_argument("--ylabel", default="")
    args = ap.parse_args()

    configs, means, stds = [], [], []
    with open(args.input, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["metric"] != args.metric:
                continue
            configs.append(row["config"])
            means.append(float(row["mean"]))
            stds.append(float(row["std"]))

    if not configs:
        print(f"Nenhuma linha para métrica '{args.metric}'")
        return 1

    fig, ax = plt.subplots(figsize=(8, 5))
    x = range(len(configs))
    ax.bar(x, means, yerr=stds, capsize=6, color="#3b82f6")
    ax.set_xticks(list(x))
    ax.set_xticklabels(configs, rotation=20, ha="right")
    ax.set_title(args.title or args.metric)
    ax.set_ylabel(args.ylabel or args.metric)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(args.output, dpi=130)
    print(f"Gráfico salvo -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
