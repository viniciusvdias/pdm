"""Shared synchronous LPA iteration loop for Ray/Dask drivers."""

from __future__ import annotations

import time
from collections.abc import Callable

import numpy as np

from graph.graph import Graph
from lpa_core.lpa import (
    count_label_changes,
    format_iter_log,
    format_summary_logs,
    shuffled_node_chunks,
)
from lpa_core.worker_memory import merge_worker_samples

ChunkResult = tuple[
    list[tuple[np.ndarray, np.ndarray]],
    list[dict[str, float | str]],
]


def run_lpa_distributed(
    graph: Graph,
    *,
    max_iter: int,
    chunk_divisor: int,
    seed: int,
    backend: str,
    run_one_iteration: Callable[[np.ndarray, list[np.ndarray]], ChunkResult],
    log_vm_peaks: Callable[[dict[str, float]], None] | None = None,
) -> tuple[np.ndarray, list[float], bool, dict[str, float]]:
    """
    Run distributed LPA with a backend-specific ``run_one_iteration`` hook.

    Returns ``(labels, iter_times, converged, worker_vm_peaks)``.

    Note — seed determinism:
        This implementation is *synchronous/batch*: all chunks read from the same
        frozen ``snapshot`` before any label is updated.  The vote kernel breaks
        ties by minimum label (deterministic).  Therefore ``seed`` only affects
        chunk boundaries, not the algorithm outcome; runs with different seeds
        produce identical partitions.  This is expected behaviour.  Seed variation
        measures temporal variance, not algorithmic variance.
    """
    n = graph.num_nodes
    labels = graph.init_labels()
    chunks = shuffled_node_chunks(n, chunk_divisor, seed)
    num_workers = len(chunks)
    worker_vm_peaks: dict[str, float] = {}
    iter_times: list[float] = []
    converged = False
    snapshot = np.empty_like(labels)

    for i in range(1, max_iter + 1):
        t_iter = time.perf_counter()
        np.copyto(snapshot, labels)
        chunk_results, samples = run_one_iteration(snapshot, chunks)
        merge_worker_samples(worker_vm_peaks, samples)
        for chunk_indices, new_labels in chunk_results:
            labels[chunk_indices] = new_labels
        changed = count_label_changes(labels, snapshot)
        elapsed = time.perf_counter() - t_iter
        iter_times.append(elapsed)
        print(
            format_iter_log(
                i, changed, n, num_workers, elapsed, backend=backend
            )
        )
        if changed == 0:
            converged = True
            break

    for line in format_summary_logs(
        labels, len(iter_times), iter_times, converged, backend=backend
    ):
        print(line)

    if worker_vm_peaks and log_vm_peaks is not None:
        log_vm_peaks(worker_vm_peaks)

    return labels, iter_times, converged, worker_vm_peaks
