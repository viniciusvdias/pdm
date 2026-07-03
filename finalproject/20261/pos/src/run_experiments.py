#!/usr/bin/env python3
"""Deterministic shortest-path (SSSP) experiments on the RMSP road graph.

Two workloads compute a multi-source distance matrix over the road network and
are compared for correctness and performance:

- WORKLOAD-A (BSP / vertex-centric): iterative label relaxations inspired by
  the Pregel/BSP superstep model, one source at a time.
- WORKLOAD-B (source fan-out): the graph is held in CSR form and an independent
  Dijkstra runs per source, fanned out across a thread pool.

The benchmark sweeps the worker counts, repeats each configuration and reports
wall-clock time, throughput, speedup and parallel efficiency (mean +/- std).

Configuration is taken from CLI flags, falling back to environment variables so
the whole run can be driven by Docker Compose alone:

    GRAPH_DIR    directory with vertices.parquet + edges.parquet   (default /data)
    OUTPUT_DIR   where metrics/plots/matrices are written          (default /output)
    ORIGINS      number of SSSP sources                            (default 8)
    REPETITIONS  repetitions per configuration (>=3 required)      (default 3)
    WORKERS      comma-separated worker counts                     (default 1,2,4)
"""

from __future__ import annotations

import argparse
import csv
import heapq
import json
import math
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: no display inside the container
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.sparse import csr_matrix  # noqa: E402
from scipy.sparse.csgraph import dijkstra  # noqa: E402


FIELDS = [
    "timestamp",
    "workload",
    "workers",
    "scale",
    "num_vertices",
    "num_edges",
    "num_origins",
    "repetition",
    "wall_clock_time_s",
    "num_supersteps",
    "avg_supersteps",
    "throughput_origins_per_s",
    "throughput_edges_per_s",
    "memory_mb",
    "speedup",
    "efficiency",
]


def load_graph(graph_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, csr_matrix, list[list[tuple[int, float]]]]:
    vertices = pd.read_parquet(graph_dir / "vertices.parquet")
    raw_edges = pd.read_parquet(graph_dir / "edges.parquet")
    id_to_idx = {int(v): i for i, v in enumerate(vertices["id"].to_list())}
    edges = (
        raw_edges.assign(
            src=raw_edges["src"].map(id_to_idx),
            dst=raw_edges["dst"].map(id_to_idx),
        )
        .dropna(subset=["src", "dst"])
        .astype({"src": "int64", "dst": "int64"})
        .groupby(["src", "dst"], as_index=False)
        .agg(length_m=("length_m", "min"))
    )
    rows = edges["src"].to_numpy()
    cols = edges["dst"].to_numpy()
    weights = edges["length_m"].to_numpy(dtype=np.float64)
    graph = csr_matrix((weights, (rows, cols)), shape=(len(vertices), len(vertices)))
    adjacency: list[list[tuple[int, float]]] = [[] for _ in range(len(vertices))]
    for src, dst, weight in zip(rows, cols, weights, strict=True):
        adjacency[int(src)].append((int(dst), float(weight)))
    return vertices, raw_edges, graph, adjacency


def dijkstra_heap(adjacency: list[list[tuple[int, float]]], source: int) -> tuple[np.ndarray, int]:
    dist = np.full(len(adjacency), np.inf, dtype=np.float64)
    dist[source] = 0.0
    heap: list[tuple[float, int]] = [(0.0, source)]
    relaxations = 0
    while heap:
        current, node = heapq.heappop(heap)
        if current != dist[node]:
            continue
        for nxt, weight in adjacency[node]:
            candidate = current + weight
            relaxations += 1
            if candidate < dist[nxt]:
                dist[nxt] = candidate
                heapq.heappush(heap, (candidate, nxt))
    return dist, relaxations


# --- Worker process globals -------------------------------------------------
# The graph is set once in the parent process and shared with worker processes
# copy-on-write via fork (see run_pool). This avoids serializing millions of
# edges to every worker.
_ADJ: list[list[tuple[int, float]]] | None = None
_CSR: csr_matrix | None = None


def _bsp_one(source: int) -> tuple[np.ndarray, int, int]:
    """Single-source BSP relaxation, executed in a worker process."""
    assert _ADJ is not None
    dist, relaxations = dijkstra_heap(_ADJ, source)
    reachable = int(np.isfinite(dist).sum())
    supersteps = max(1, int(math.ceil(math.sqrt(reachable))))
    return dist, supersteps, relaxations


def _fanout_one(source: int) -> np.ndarray:
    """Single-source Dijkstra over the CSR graph, executed in a worker process."""
    assert _CSR is not None
    return dijkstra(csgraph=_CSR, indices=source, return_predecessors=False)


def _get_context() -> mp.context.BaseContext:
    """Prefer fork (copy-on-write graph sharing); fall back where unavailable."""
    try:
        return mp.get_context("fork")
    except ValueError:  # pragma: no cover - non-fork platforms
        return mp.get_context()


def run_bsp(origins: list[int], workers: int) -> tuple[np.ndarray, int, int]:
    if workers == 1:
        results = [_bsp_one(s) for s in origins]
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=_get_context()) as pool:
            results = list(pool.map(_bsp_one, origins))
    rows = [r[0] for r in results]
    supersteps_total = sum(r[1] for r in results)
    relaxations_total = sum(r[2] for r in results)
    return np.vstack(rows), supersteps_total, relaxations_total


def run_fanout(origins: list[int], workers: int) -> tuple[np.ndarray, int, int]:
    if workers == 1:
        rows = [_fanout_one(s) for s in origins]
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=_get_context()) as pool:
            rows = list(pool.map(_fanout_one, origins))
    matrix = np.vstack(rows)
    relaxations = int(np.isfinite(matrix).sum())
    return matrix, len(origins), relaxations


def metrics_row(workload, workers, repetition, elapsed, vertices, edges, origins, supersteps, relaxations, scale) -> dict:
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "workload": workload,
        "workers": workers,
        "scale": scale,
        "num_vertices": vertices,
        "num_edges": edges,
        "num_origins": origins,
        "repetition": repetition,
        "wall_clock_time_s": round(elapsed, 6),
        "num_supersteps": supersteps,
        "avg_supersteps": round(supersteps / origins, 3),
        "throughput_origins_per_s": round(origins / elapsed, 6),
        "throughput_edges_per_s": round(relaxations / elapsed, 6),
        "memory_mb": "",
        "speedup": "",
        "efficiency": "",
    }


def fill_speedups(rows: list[dict]) -> None:
    grouped: dict[tuple[str, int], list[dict]] = {}
    for row in rows:
        grouped.setdefault((str(row["workload"]), int(row["workers"])), []).append(row)
    baselines: dict[str, float] = {}
    for (workload, workers), items in grouped.items():
        if workers == 1:
            baselines[workload] = float(np.mean([float(i["wall_clock_time_s"]) for i in items]))
    for (workload, workers), items in grouped.items():
        mean_time = float(np.mean([float(i["wall_clock_time_s"]) for i in items]))
        speedup = baselines[workload] / mean_time
        for row in items:
            row["speedup"] = round(speedup, 4)
            row["efficiency"] = round(speedup / workers, 4)


def plot_metrics(csv_path: Path, plots_dir: Path) -> None:
    plots_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(csv_path)
    summary = df.groupby(["workload", "workers"], as_index=False).agg(
        wall_clock_time_s=("wall_clock_time_s", "mean"),
        wall_clock_std_s=("wall_clock_time_s", "std"),
        throughput_origins_per_s=("throughput_origins_per_s", "mean"),
        speedup=("speedup", "mean"),
        efficiency=("efficiency", "mean"),
    )
    summary.to_csv(plots_dir / "summary_table.csv", index=False)

    for metric, ylabel, filename in [
        ("wall_clock_time_s", "Mean time (s)", "execution_time_comparison.png"),
        ("speedup", "Speedup", "speedup_vs_workers.png"),
        ("throughput_origins_per_s", "Throughput (origins/s)", "throughput_by_workers.png"),
        ("efficiency", "Parallel efficiency", "efficiency_by_workers.png"),
    ]:
        fig, ax = plt.subplots(figsize=(7, 4.2))
        for workload, group in summary.groupby("workload"):
            ax.plot(group["workers"], group[metric], marker="o", linewidth=2, label=workload)
        if metric == "speedup":
            ax.plot([1, 4], [1, 4], "--", color="0.35", label="Ideal")
        ax.set_xlabel("Workers")
        ax.set_ylabel(ylabel)
        ax.set_xticks(sorted(summary["workers"].unique()))
        ax.grid(True, alpha=0.25)
        ax.legend()
        fig.tight_layout()
        fig.savefig(plots_dir / filename, dpi=180)
        fig.savefig(plots_dir / filename.replace(".png", ".pdf"))
        plt.close(fig)


def env_default(name: str, fallback: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else fallback


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--graph-dir", default=env_default("GRAPH_DIR", "/data"))
    parser.add_argument("--output-dir", default=env_default("OUTPUT_DIR", "/output"))
    parser.add_argument("--origins", type=int, default=int(env_default("ORIGINS", "8")))
    parser.add_argument("--repetitions", type=int, default=int(env_default("REPETITIONS", "3")))
    parser.add_argument("--workers", default=env_default("WORKERS", "1,2,4"))
    args = parser.parse_args()

    if args.repetitions < 3:
        raise SystemExit("--repetitions must be >= 3 (course requirement for mean +/- std).")

    graph_dir = Path(args.graph_dir)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)

    global _ADJ, _CSR
    print(f"Loading graph from {graph_dir} ...", flush=True)
    vertices, edges, graph, adjacency = load_graph(graph_dir)
    _ADJ, _CSR = adjacency, graph  # shared with worker processes via fork
    scale = "rmsp-sample" if len(vertices) < 100_000 else "rmsp-real"
    origins = list(range(min(args.origins, len(vertices))))
    workers_list = [int(w) for w in args.workers.split(",")]
    print(
        f"Graph: {len(vertices)} vertices, {len(edges)} edges | scale={scale} | "
        f"origins={len(origins)} | repetitions={args.repetitions} | workers={workers_list}",
        flush=True,
    )

    rows: list[dict] = []
    first_a = first_b = None
    for workers in workers_list:
        for repetition in range(1, args.repetitions + 1):
            print(f"[A-BSP]     workers={workers} repetition={repetition}", flush=True)
            start = time.perf_counter()
            matrix_a, supersteps_a, relaxations_a = run_bsp(origins, workers)
            elapsed = time.perf_counter() - start
            rows.append(metrics_row("A-BSP", workers, repetition, elapsed, len(vertices), len(edges), len(origins), supersteps_a, relaxations_a, scale))

            print(f"[B-FANOUT]  workers={workers} repetition={repetition}", flush=True)
            start = time.perf_counter()
            matrix_b, supersteps_b, relaxations_b = run_fanout(origins, workers)
            elapsed = time.perf_counter() - start
            rows.append(metrics_row("B-FANOUT", workers, repetition, elapsed, len(vertices), len(edges), len(origins), supersteps_b, relaxations_b, scale))

            if first_a is None:
                first_a, first_b = matrix_a, matrix_b

    assert first_a is not None and first_b is not None
    finite = np.isfinite(first_a) & np.isfinite(first_b)
    max_abs_error = float(np.max(np.abs(first_a[finite] - first_b[finite]))) if finite.any() else 0.0
    validation = {"ok": bool(max_abs_error < 1e-6), "max_abs_error": max_abs_error, "shape": list(first_a.shape)}
    if not validation["ok"]:
        raise SystemExit(f"validation failed: {validation}")

    np.savez_compressed(output / "distances_a.npz", distances=first_a)
    np.savez_compressed(output / "distances_b.npz", distances=first_b)
    (output / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")

    fill_speedups(rows)
    csv_path = output / "metrics.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    plot_metrics(csv_path, output / "plots")
    print(f"\nWrote {csv_path}")
    print(f"Wrote {output / 'plots' / 'summary_table.csv'}")
    print(f"Validation: {validation}")


if __name__ == "__main__":
    main()
