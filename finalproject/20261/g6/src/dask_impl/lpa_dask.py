"""Distributed Label Propagation using Dask."""

from __future__ import annotations

import time

import numpy as np
from dask.distributed import Client, LocalCluster

from config import AppConfig, effective_dask_workers, load_config
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

_BACKEND = "dask"


def _scheduler_url(address: str) -> str:
    if address.startswith("tcp://"):
        return address
    return f"tcp://{address}"


def _connect_dask(
    cfg: AppConfig,
    n_workers: int | None,
    expected_workers: int,
) -> tuple[Client, LocalCluster | None]:
    if cfg.dask_scheduler_address:
        client = Client(_scheduler_url(cfg.dask_scheduler_address))
        try:
            client.wait_for_workers(n_workers=expected_workers, timeout=120)
        except Exception:
            registered = len(client.scheduler_info().get("workers", {}))
            print(
                format_runtime_log(
                    _BACKEND,
                    "warn",
                    workers=registered,
                    expected=f">={expected_workers}",
                    timeout_s="120s",
                )
            )
        registered = len(client.scheduler_info().get("workers", {}))
        print(
            format_runtime_log(
                _BACKEND,
                "cluster",
                workers=registered,
                expected=f">={expected_workers}",
            )
        )
        return client, None

    cluster = LocalCluster(
        n_workers=n_workers or expected_workers,
        threads_per_worker=1,
        processes=True,
        silence_logs=True,
    )
    return Client(cluster), cluster


def run_lpa_dask(
    graph: Graph,
    max_iter: int = 100,
    chunk_divisor: int = 12,
    n_workers: int | None = None,
    cfg: AppConfig | None = None,
    seed: int = 42,
) -> LpaResult:
    cfg = cfg or load_config()
    workers = n_workers if n_workers is not None else effective_dask_workers(cfg)

    n = graph.num_nodes
    num_workers = len(node_chunks(n, chunk_divisor))

    t0 = time.perf_counter()
    client, cluster = _connect_dask(cfg, workers, num_workers)
    init_time = time.perf_counter() - t0

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

    graph_ref = client.scatter(graph, broadcast=True)

    def run_one_iteration(
        snapshot: np.ndarray,
        chunk_list: list[np.ndarray],
    ) -> tuple[list[tuple[np.ndarray, np.ndarray]], list[dict[str, float | str]]]:
        snapshot_ref = client.scatter(snapshot, broadcast=True)
        futures = [
            client.submit(lpa_iteration_chunk_tracked, chunk, graph_ref, snapshot_ref)
            for chunk in chunk_list
        ]
        results = client.gather(futures)
        chunk_results = [
            (chunk, new_labels)
            for chunk, (new_labels, _sample) in zip(chunk_list, results, strict=True)
        ]
        samples = [sample for _new_labels, sample in results]
        return chunk_results, samples

    try:
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
    finally:
        client.close()
        if cluster is not None:
            cluster.close()
