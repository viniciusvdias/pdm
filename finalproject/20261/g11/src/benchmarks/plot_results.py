#!/usr/bin/env python3
"""Gera graficos de barras (throughput e latencia) dos cenarios A e B,
com barras de erro = desvio padrao. Le benchmarks/results/summary.csv.

Uso: python benchmarks/plot_results.py
Saida: benchmarks/results/figs/*.png
"""
import csv
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
SUMMARY = os.path.join(HERE, "results", "summary.csv")
OUT = os.path.join(HERE, "results", "figs")
os.makedirs(OUT, exist_ok=True)

# Cores consistentes
C_TP = "#2b7bba"   # throughput (azul)
C_LAT = "#c0504d"  # latencia (vermelho)


def load():
    rows = []
    with open(SUMMARY, newline="") as fh:
        for r in csv.DictReader(fh):
            r = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
            rows.append(r)
    return rows


def bar(ax, x, y, yerr, color, ylabel, title, xlabel):
    bars = ax.bar(x, y, yerr=yerr, capsize=6, color=color, alpha=0.85,
                  edgecolor="black", linewidth=0.6)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.set_title(title, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    for b, val in zip(bars, y):
        ax.annotate(f"{val:,.0f}", (b.get_x() + b.get_width() / 2, val),
                    ha="center", va="bottom", fontsize=9, xytext=(0, 3),
                    textcoords="offset points")
    return bars


def main():
    rows = load()
    A = sorted([r for r in rows if r["scenario"] == "A"], key=lambda r: int(r["value"]))
    B = sorted([r for r in rows if r["scenario"] == "B"], key=lambda r: int(r["value"]))

    # --- Cenario A: throughput e latencia por cores ---
    xa = [f"{r['value']} core(s)" for r in A]
    tpa = [float(r["throughput_rps_mean"]) for r in A]
    tpa_e = [float(r["throughput_rps_std"]) for r in A]
    lata = [float(r["latency_ms_mean"]) for r in A]
    lata_e = [float(r["latency_ms_std"]) for r in A]

    fig, ax = plt.subplots(figsize=(6, 4.2))
    bar(ax, xa, tpa, tpa_e, C_TP, "Throughput (registros/s)",
        "Cenário A — Throughput vs cores do executor", "spark.executor.cores")
    fig.tight_layout(); fig.savefig(f"{OUT}/A_throughput.png", dpi=150); plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4.2))
    bar(ax, xa, lata, lata_e, C_LAT, "Latência de micro-batch (ms)",
        "Cenário A — Latência vs cores do executor", "spark.executor.cores")
    fig.tight_layout(); fig.savefig(f"{OUT}/A_latency.png", dpi=150); plt.close(fig)

    # --- Cenario B: throughput e latencia por workers ---
    xb = [f"{r['value']} worker(s)" for r in B]
    tpb = [float(r["throughput_rps_mean"]) for r in B]
    tpb_e = [float(r["throughput_rps_std"]) for r in B]
    latb = [float(r["latency_ms_mean"]) for r in B]
    latb_e = [float(r["latency_ms_std"]) for r in B]

    fig, ax = plt.subplots(figsize=(6, 4.2))
    bar(ax, xb, tpb, tpb_e, C_TP, "Throughput (registros/s)",
        "Cenário B — Throughput vs número de workers", "nº de spark-workers (4 cores cada)")
    fig.tight_layout(); fig.savefig(f"{OUT}/B_throughput.png", dpi=150); plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4.2))
    bar(ax, xb, latb, latb_e, C_LAT, "Latência de micro-batch (ms)",
        "Cenário B — Latência vs número de workers", "nº de spark-workers (4 cores cada)")
    fig.tight_layout(); fig.savefig(f"{OUT}/B_latency.png", dpi=150); plt.close(fig)

    # --- Painel combinado (1 figura, 4 subplots) p/ o slide ---
    fig, axs = plt.subplots(2, 2, figsize=(11, 8))
    bar(axs[0, 0], xa, tpa, tpa_e, C_TP, "Throughput (reg/s)", "A — Throughput vs cores", "cores")
    bar(axs[0, 1], xb, tpb, tpb_e, C_TP, "Throughput (reg/s)", "B — Throughput vs workers", "workers")
    bar(axs[1, 0], xa, lata, lata_e, C_LAT, "Latência (ms)", "A — Latência vs cores", "cores")
    bar(axs[1, 1], xb, latb, latb_e, C_LAT, "Latência (ms)", "B — Latência vs workers", "workers")
    fig.suptitle("Avaliação de desempenho — média ± desvio padrão (3 repetições)",
                 fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(f"{OUT}/painel_completo.png", dpi=150); plt.close(fig)

    print("Gráficos gerados em", OUT)
    for f in sorted(os.listdir(OUT)):
        print("  -", f)


if __name__ == "__main__":
    main()
