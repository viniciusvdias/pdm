"""Distributed Label Propagation using Ray."""

from __future__ import annotations

import logging
import time

import numpy as np
import ray

from graph.graph import Graph
from lpa_core.distributed import run_lpa_distributed
from lpa_core.lpa import (
    LpaResult,
    format_run_start,
    format_runtime_log,
    log_vm_peaks,
    lpa_iteration_chunk_tracked,
    node_chunks,
)

_BACKEND = "ray"


@ray.remote
def lpa_chunk_remote(
    chunk_indices: np.ndarray,
    graph: Graph,
    labels_snapshot: np.ndarray,
) -> tuple[np.ndarray, dict[str, float | str]]:
    return lpa_iteration_chunk_tracked(chunk_indices, graph, labels_snapshot)


def _ray_client_address(ray_head_address: str) -> str:
    host = ray_head_address.split(":")[0]
    return f"ray://{host}:10001"


def _wait_for_ray_workers(expected: int, timeout_s: float = 120.0) -> int:
    """Block until cluster CPUs are available (remote workers registered)."""
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        cpus = int(ray.cluster_resources().get("CPU", 0))
        if cpus >= expected:
            print(
                format_runtime_log(
                    _BACKEND, "cluster", workers=cpus, expected=f">={expected}"
                )
            )
            return cpus
        time.sleep(2.0)
    cpus = int(ray.cluster_resources().get("CPU", 0))
    print(
        format_runtime_log(
            _BACKEND,
            "warn",
            workers=cpus,
            expected=f">={expected}",
            timeout_s=f"{timeout_s:.0f}s",
        )
    )
    return cpus


def run_lpa_ray(
    graph: Graph,
    max_iter: int = 100,
    chunk_divisor: int = 12,
    num_cpus: int | None = None,
    ray_head_address: str | None = None,
    seed: int = 42,
) -> LpaResult:
    n = graph.num_nodes
    num_workers = len(node_chunks(n, chunk_divisor))

    t0 = time.perf_counter()
    if not ray.is_initialized():
        if ray_head_address:
            ray.init(
                address=_ray_client_address(ray_head_address),
                ignore_reinit_error=True,
                logging_level=logging.WARNING,
            )
        else:
            cpus = num_cpus if num_cpus is not None else chunk_divisor
            ray.init(
                num_cpus=cpus,
                ignore_reinit_error=True,
                logging_level=logging.WARNING,
            )
    init_time = time.perf_counter() - t0

    if ray_head_address:
        _wait_for_ray_workers(max(1, num_workers))

    print(
        format_run_start(
            _BACKEND,
            nodes=n,
            chunks=num_workers,
            max_iter=max_iter,
            init_s=init_time,
            seed=seed,
        )
    )

    graph_ref = ray.put(graph)

    def run_one_iteration(
        snapshot: np.ndarray,
        chunk_list: list[np.ndarray],
    ) -> tuple[list[tuple[np.ndarray, np.ndarray]], list[dict[str, float | str]]]:
        snapshot_ref = ray.put(snapshot)
        refs = [
            lpa_chunk_remote.remote(chunk, graph_ref, snapshot_ref)
            for chunk in chunk_list
        ]
        results = ray.get(refs)
        chunk_results = [
            (chunk, new_labels)
            for chunk, (new_labels, _sample) in zip(chunk_list, results, strict=True)
        ]
        samples = [sample for _new_labels, sample in results]
        return chunk_results, samples

    labels, iter_times, converged, worker_vm_peaks = run_lpa_distributed(
        graph,
        max_iter=max_iter,
        chunk_divisor=chunk_divisor,
        seed=seed,
        backend=_BACKEND,
        run_one_iteration=run_one_iteration,
        log_vm_peaks=lambda peaks: log_vm_peaks(_BACKEND, peaks),
    )

    return LpaResult(
        num_communities=len(np.unique(labels)),
        num_levels=len(iter_times),
        init_time_s=init_time,
        algorithm_time_s=sum(iter_times),
        level_times_s=iter_times,
        converged=converged,
        num_workers=num_workers,
        node_count=n,
        partition_node_ids=graph.node_ids,
        partition_labels=labels,
        worker_vm_peaks_mb=worker_vm_peaks,
    )
