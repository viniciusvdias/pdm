#!/usr/bin/env python3
"""Graficos COMPARATIVOS host unico (12 cores) vs cluster multi-VM (AWS Swarm).

Foco no Cenario B (workers 1->2->3), lado a lado, para o relatorio distribuido.
Tambem gera os graficos do cenario AWS isolado (A e B) reaproveitando a estetica
de plot_results.py.

Le:
  benchmarks/results/summary_FINAL.csv  (host unico, 12 cores)
  benchmarks/results/summary_AWS.csv    (cluster 4 VMs)
Saida:
  benchmarks/results/figs/compare_B_throughput.png
  benchmarks/results/figs/compare_B_latency.png
  benchmarks/results/figs/compare_B_painel.png
  benchmarks/results/figs/AWS_B_throughput.png
  benchmarks/results/figs/AWS_B_latency.png
  benchmarks/results/figs/AWS_painel.png
"""
import csv
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
OUT = os.path.join(RES, "figs")
os.makedirs(OUT, exist_ok=True)

C_HOST = "#7f8c8d"   # host unico (cinza)
C_AWS = "#2b7bba"    # cluster AWS (azul)
C_TP = "#2b7bba"
C_LAT = "#c0504d"


def load(name):
    rows = []
    with open(os.path.join(RES, name), newline="") as fh:
        for r in csv.DictReader(fh):
            rows.append({k.strip(): (v.strip() if isinstance(v, str) else v)
                         for k, v in r.items()})
    return rows


def scen(rows, s):
    return sorted([r for r in rows if r["scenario"] == s],
                  key=lambda r: int(r["value"]))


def grouped_bar(ax, labels, series, ylabel, title, xlabel):
    """series: list of (name, values, errs, color)."""
    x = np.arange(len(labels))
    n = len(series)
    w = 0.8 / n
    for i, (name, vals, errs, color) in enumerate(series):
        off = (i - (n - 1) / 2) * w
        bars = ax.bar(x + off, vals, w, yerr=errs, capsize=5, label=name,
                      color=color, alpha=0.88, edgecolor="black", linewidth=0.6)
        for b, v in zip(bars, vals):
            ax.annotate(f"{v:,.0f}", (b.get_x() + b.get_width() / 2, v),
                        ha="center", va="bottom", fontsize=8,
                        xytext=(0, 2), textcoords="offset points")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.set_title(title, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend()


def main():
    host = load("summary_FINAL.csv")
    aws = load("summary_AWS.csv")

    # --- Cenario B: host vs AWS (workers 1,2,3 em ambos) ---
    Bh, Ba = scen(host, "B"), scen(aws, "B")
    labels = [f"{r['value']}" for r in Ba]  # 1,2,3

    tp_h = [float(r["throughput_rps_mean"]) for r in Bh]
    tp_he = [float(r["throughput_rps_std"]) for r in Bh]
    tp_a = [float(r["throughput_rps_mean"]) for r in Ba]
    tp_ae = [float(r["throughput_rps_std"]) for r in Ba]

    lat_h = [float(r["latency_ms_mean"]) for r in Bh]
    lat_he = [float(r["latency_ms_std"]) for r in Bh]
    lat_a = [float(r["latency_ms_mean"]) for r in Ba]
    lat_ae = [float(r["latency_ms_std"]) for r in Ba]

    # Throughput comparativo
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    grouped_bar(ax, labels, [
        ("Host único (12 cores)", tp_h, tp_he, C_HOST),
        ("Cluster AWS (4 VMs)", tp_a, tp_ae, C_AWS),
    ], "Throughput (registros/s)",
        "Cenário B — Throughput: host único vs cluster multi-VM",
        "nº de spark-workers")
    fig.tight_layout(); fig.savefig(f"{OUT}/compare_B_throughput.png", dpi=150)
    plt.close(fig)

    # Latencia comparativa
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    grouped_bar(ax, labels, [
        ("Host único (12 cores)", lat_h, lat_he, C_HOST),
        ("Cluster AWS (4 VMs)", lat_a, lat_ae, C_AWS),
    ], "Latência de micro-batch (ms)",
        "Cenário B — Latência: host único vs cluster multi-VM",
        "nº de spark-workers")
    fig.tight_layout(); fig.savefig(f"{OUT}/compare_B_latency.png", dpi=150)
    plt.close(fig)

    # Painel comparativo (throughput + latencia + escalabilidade relativa)
    fig, axs = plt.subplots(1, 3, figsize=(16, 4.8))
    grouped_bar(axs[0], labels, [
        ("Host único", tp_h, tp_he, C_HOST),
        ("Cluster AWS", tp_a, tp_ae, C_AWS),
    ], "Throughput (reg/s)", "Throughput vs workers", "workers")
    grouped_bar(axs[1], labels, [
        ("Host único", lat_h, lat_he, C_HOST),
        ("Cluster AWS", lat_a, lat_ae, C_AWS),
    ], "Latência (ms)", "Latência vs workers", "workers")

    # Escalabilidade relativa (throughput normalizado por 1 worker)
    rel_h = [v / tp_h[0] for v in tp_h]
    rel_a = [v / tp_a[0] for v in tp_a]
    ax = axs[2]
    ax.plot(labels, rel_h, "o-", color=C_HOST, label="Host único", linewidth=2)
    ax.plot(labels, rel_a, "s-", color=C_AWS, label="Cluster AWS", linewidth=2)
    ax.axhline(1.0, color="black", linestyle=":", alpha=0.5)
    for xx, v in zip(labels, rel_h):
        ax.annotate(f"{v:.2f}×", (xx, v), fontsize=8, ha="center", va="top",
                    xytext=(0, -6), textcoords="offset points", color=C_HOST)
    for xx, v in zip(labels, rel_a):
        ax.annotate(f"{v:.2f}×", (xx, v), fontsize=8, ha="center", va="bottom",
                    xytext=(0, 6), textcoords="offset points", color=C_AWS)
    ax.set_title("Speedup relativo (vs 1 worker)", fontweight="bold")
    ax.set_xlabel("workers"); ax.set_ylabel("throughput / throughput(1w)")
    ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend()

    fig.suptitle("Cenário B — Escala horizontal: host único (cores compartilhados) "
                 "vs cluster AWS (VMs físicas)", fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(f"{OUT}/compare_B_painel.png", dpi=150); plt.close(fig)

    # --- Cenario AWS isolado (A por cores, B por workers) ---
    Aa = scen(aws, "A")
    xa = [f"{r['value']} core(s)" for r in Aa]
    tpa = [float(r["throughput_rps_mean"]) for r in Aa]
    tpa_e = [float(r["throughput_rps_std"]) for r in Aa]
    lata = [float(r["latency_ms_mean"]) for r in Aa]
    lata_e = [float(r["latency_ms_std"]) for r in Aa]
    xb = [f"{r['value']} worker(s)" for r in Ba]

    def simple_bar(ax, x, y, yerr, color, ylabel, title, xlabel):
        bars = ax.bar(x, y, yerr=yerr, capsize=6, color=color, alpha=0.85,
                      edgecolor="black", linewidth=0.6)
        ax.set_ylabel(ylabel); ax.set_xlabel(xlabel)
        ax.set_title(title, fontweight="bold")
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        for b, v in zip(bars, y):
            ax.annotate(f"{v:,.0f}", (b.get_x() + b.get_width() / 2, v),
                        ha="center", va="bottom", fontsize=9,
                        xytext=(0, 3), textcoords="offset points")

    fig, ax = plt.subplots(figsize=(6, 4.2))
    simple_bar(ax, xb, tp_a, tp_ae, C_TP, "Throughput (registros/s)",
               "AWS Cenário B — Throughput vs workers (1 VM cada)",
               "nº de spark-workers (VMs t3.medium)")
    fig.tight_layout(); fig.savefig(f"{OUT}/AWS_B_throughput.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4.2))
    simple_bar(ax, xb, lat_a, lat_ae, C_LAT, "Latência de micro-batch (ms)",
               "AWS Cenário B — Latência vs workers (1 VM cada)",
               "nº de spark-workers (VMs t3.medium)")
    fig.tight_layout(); fig.savefig(f"{OUT}/AWS_B_latency.png", dpi=150)
    plt.close(fig)

    fig, axs = plt.subplots(2, 2, figsize=(11, 8))
    simple_bar(axs[0, 0], xa, tpa, tpa_e, C_TP, "Throughput (reg/s)",
               "AWS A — Throughput vs cores", "cores (1 VM)")
    simple_bar(axs[0, 1], xb, tp_a, tp_ae, C_TP, "Throughput (reg/s)",
               "AWS B — Throughput vs workers", "workers (VMs)")
    simple_bar(axs[1, 0], xa, lata, lata_e, C_LAT, "Latência (ms)",
               "AWS A — Latência vs cores", "cores (1 VM)")
    simple_bar(axs[1, 1], xb, lat_a, lat_ae, C_LAT, "Latência (ms)",
               "AWS B — Latência vs workers", "workers (VMs)")
    fig.suptitle("Cluster AWS Swarm (4 VMs) — média ± desvio padrão (3 repetições)",
                 fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(f"{OUT}/AWS_painel.png", dpi=150); plt.close(fig)

    print("Gráficos comparativos gerados em", OUT)
    for f in sorted(os.listdir(OUT)):
        print("  -", f)


if __name__ == "__main__":
    main()
