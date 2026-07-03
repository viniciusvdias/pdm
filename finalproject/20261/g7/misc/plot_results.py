import math
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Data com os tempos de processamento (Aplicação Manual)
data = {
    "WORKLOAD-1\n(Weapon Effectiveness)": {
        1: [23.529, 24.644, 22.050, 20.390, 21.330],
        2: [25.805, 24.891, 23.426, 23.678, 24.326],
        4: [24.252, 23.117, 22.486, 23.173, 22.877],
    },
    "WORKLOAD-2\n(Rank vs Performance)": {
        1: [17.608, 17.228, 16.336, 16.772, 20.701],
        2: [18.221, 14.298, 15.808, 17.505, 17.696],
        4: [12.353, 17.042, 16.106, 15.414, 18.157],
    },
    "WORKLOAD-3\n(Side Advantage CT vs T)": {
        1: [20.658, 19.487, 12.359, 14.156, 13.978],
        2: [15.310, 17.653, 13.578, 13.471, 18.085],
        4: [15.556, 14.062, 11.579, 15.861, 17.158],
    },
}

workers = [1, 2, 4]
colors = ["#4C72B0", "#DD8452", "#55A868"]

fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=False)

fig.suptitle(
    "Spark Workload Execution Time (10,538,182 rows)",
    fontsize=15,
    fontweight="bold",
)

for ax, (title, wdata) in zip(axes, data.items()):

    avgs = []
    stds = []

    for w in workers:
        times = wdata[w]
        n = len(times)

        avg = sum(times) / n
        std = math.sqrt(sum((t - avg) ** 2 for t in times) / (n - 1))

        avgs.append(avg)
        stds.append(std)

    bars = ax.bar(
        [str(w) for w in workers],
        avgs,
        width=0.60,
        yerr=stds,
        capsize=6,
        color=colors,
        edgecolor="white",
        linewidth=1,
        error_kw={
            "elinewidth": 2,
            "ecolor": "#222222",
        },
    )

    # Valor da média dentro da barra
    for bar, avg in zip(bars, avgs):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            avg - 0.25,
            f"{avg:.2f}s",
            ha="center",
            va="top",
            color="white",
            fontsize=10,
            fontweight="bold",
        )

    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel("Workers", fontsize=10)
    ax.set_ylabel("Time (s)", fontsize=10)

    # Escala dinâmica
    menor = min(avg - std for avg, std in zip(avgs, stds))
    maior = max(avg + std for avg, std in zip(avgs, stds))

    margem = 0.8

    ax.set_ylim(menor - margem, maior + margem)

    ax.grid(axis="y", linestyle="--", linewidth=0.8, alpha=0.6)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.95])

# Diretório de saída
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "output")

os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, "benchmark_results.png")

plt.savefig(output_file, dpi=300, bbox_inches="tight")

print(f"Saved: {output_file}")