import math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

data = {
    "WORKLOAD-1\n(Weapon Effectiveness)": {
        1: [2.024, 2.265, 1.969, 2.006, 2.030],
        2: [2.323, 1.738, 1.904, 2.045, 1.951],
        4: [1.917, 2.020, 1.903, 1.889, 1.700],
    },
    "WORKLOAD-2\n(Rank vs Performance)": {
        1: [1.787, 1.703, 1.472, 1.637, 1.528],
        2: [1.475, 1.440, 1.596, 1.830, 1.719],
        4: [1.437, 1.352, 1.587, 1.619, 1.496],
    },
    "WORKLOAD-3\n(Side Advantage CT vs T)": {
        1: [1.270, 1.671, 1.375, 1.309, 1.164],
        2: [1.164, 1.416, 1.224, 1.751, 1.555],
        4: [1.414, 1.085, 1.503, 1.605, 1.224],
    },
}

workers = [1, 2, 4]
colors = ["#4C72B0", "#DD8452", "#55A868"]

fig, axes = plt.subplots(1, 3, figsize=(14, 5), sharey=False)
fig.suptitle("Spark Workload Execution Time (5,992,097 rows)", fontsize=13, fontweight="bold")

for ax, (title, wdata) in zip(axes, data.items()):
    avgs, stds = [], []
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
        yerr=stds,
        capsize=6,
        color=colors,
        edgecolor="white",
        linewidth=0.8,
        error_kw={"elinewidth": 1.5, "ecolor": "#333333"},
    )

    for bar, avg, std in zip(bars, avgs, stds):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            avg + std + 0.03,
            f"{avg:.2f}s",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_title(title, fontsize=10)
    ax.set_xlabel("Workers", fontsize=9)
    ax.set_ylabel("Time (s)", fontsize=9)
    ax.set_ylim(0, max(avgs) + max(stds) + 0.4)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
plt.savefig("output/benchmark_results.png", dpi=150, bbox_inches="tight")
print("Saved: output/benchmark_results.png")
