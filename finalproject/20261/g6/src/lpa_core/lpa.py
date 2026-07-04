"""Label Propagation core (pure Python, no Ray/Dask)."""

from __future__ import annotations

import random
import statistics
import time
from dataclasses import dataclass, field
from typing import Callable

import numpy as np
from numba import njit, types
from numba.typed import Dict

from graph.graph import Graph
from lpa_core.worker_memory import sample_worker_peak, worker_host_id


@dataclass
class LpaResult:
    num_communities: int
    num_levels: int
    init_time_s: float
    algorithm_time_s: float
    level_times_s: list[float] = field(default_factory=list)
    converged: bool = True
    num_workers: int = 1
    node_count: int = 0
    labels: dict[int, int] = field(default_factory=dict)
    partition_node_ids: np.ndarray | None = None
    partition_labels: np.ndarray | None = None
    worker_vm_peaks_mb: dict[str, float] = field(default_factory=dict)

    def labels_dict(self, graph: Graph | None = None) -> dict[int, int]:
        """Export labels as dict (small graphs only; avoids huge dicts)."""
        if self.labels:
            return self.labels
        if self.partition_node_ids is None or self.partition_labels is None:
            return {}
        if graph is not None:
            return graph.labels_to_dict(self.partition_labels)
        if len(self.partition_node_ids) > 50_000:
            return {}
        return {
            int(n): int(label)
            for n, label in zip(
                self.partition_node_ids, self.partition_labels, strict=True
            )
        }

    def partition_arrays(self) -> tuple[np.ndarray, np.ndarray] | None:
        """Node ids and label vector for partition export."""
        if self.partition_node_ids is not None and self.partition_labels is not None:
            return self.partition_node_ids, self.partition_labels
        if not self.labels:
            return None
        nodes = sorted(self.labels.keys())
        return (
            np.asarray(nodes, dtype=np.int64),
            np.asarray([self.labels[n] for n in nodes], dtype=np.int64),
        )

    @property
    def total_time_s(self) -> float:
        return self.init_time_s + self.algorithm_time_s

    def summary_dict(self) -> dict:
        """JSON-safe summary without the full label map."""
        return {
            "num_communities": self.num_communities,
            "num_levels": self.num_levels,
            "init_time_s": self.init_time_s,
            "algorithm_time_s": self.algorithm_time_s,
            "total_time_s": self.total_time_s,
            "level_times_s": self.level_times_s,
            "converged": self.converged,
            "num_workers": self.num_workers,
            "node_count": self.node_count,
        }


def _make_chunks(order: np.ndarray, divisor: int) -> list[np.ndarray]:
    n = len(order)
    num_chunks = min(max(1, divisor), n)
    chunk_size = (n + num_chunks - 1) // num_chunks
    return [order[i : i + chunk_size] for i in range(0, n, chunk_size)]


def node_chunks(node_count: int, chunk_divisor: int) -> list[np.ndarray]:
    """Split node indices into ~chunk_divisor chunks of roughly equal size."""
    if node_count == 0:
        return []
    return _make_chunks(np.arange(node_count, dtype=np.int64), chunk_divisor)


def shuffled_node_chunks(
    node_count: int,
    chunk_divisor: int,
    seed: int,
) -> list[np.ndarray]:
    """Like ``node_chunks`` but with a reproducible random node order."""
    if node_count == 0:
        return []
    order = np.arange(node_count, dtype=np.int64)
    np.random.default_rng(seed).shuffle(order)
    return _make_chunks(order, chunk_divisor)


@njit(cache=True)
def _vote_kernel(
    chunk_indices: np.ndarray,
    indptr: np.ndarray,
    neighbors: np.ndarray,
    labels: np.ndarray,
    out: np.ndarray,
) -> None:
    for pos in range(len(chunk_indices)):
        idx = chunk_indices[pos]
        start = indptr[idx]
        end = indptr[idx + 1]
        if start == end:
            out[pos] = labels[idx]
            continue
        best_label = labels[idx]
        best_count = np.int64(0)
        votes = Dict.empty(key_type=types.int64, value_type=types.int64)
        for k in range(start, end):
            lbl = labels[neighbors[k]]
            cnt = votes.get(lbl, 0) + 1
            votes[lbl] = cnt
            if cnt > best_count or (cnt == best_count and lbl < best_label):
                best_count = cnt
                best_label = lbl
        out[pos] = best_label


_warmup_idx = np.zeros(1, dtype=np.int64)
_warmup_iptr = np.array([0, 0], dtype=np.int64)
_warmup_nbr = np.empty(0, dtype=np.int64)
_warmup_lbl = np.zeros(1, dtype=np.int64)
_warmup_out = np.empty(1, dtype=np.int64)
_vote_kernel(_warmup_idx, _warmup_iptr, _warmup_nbr, _warmup_lbl, _warmup_out)


def lpa_iteration_chunk(
    chunk_indices: np.ndarray,
    graph: Graph,
    labels_snapshot: np.ndarray,
) -> np.ndarray:
    """Return new labels for ``chunk_indices`` from a frozen global snapshot."""
    out = np.empty(len(chunk_indices), dtype=np.int64)
    _vote_kernel(
        chunk_indices.astype(np.int64, copy=False),
        graph.indptr,
        graph.neighbors,
        labels_snapshot.astype(np.int64, copy=False),
        out,
    )
    return out


def lpa_iteration_chunk_tracked(
    chunk_indices: np.ndarray,
    graph: Graph,
    labels_snapshot: np.ndarray,
) -> tuple[np.ndarray, dict[str, float | str]]:
    """Like ``lpa_iteration_chunk`` but returns worker RSS/host for VM metrics."""
    sample_worker_peak()
    out = lpa_iteration_chunk(chunk_indices, graph, labels_snapshot)
    peak = sample_worker_peak()
    return out, {"host": worker_host_id(), "peak_rss_mb": peak}


def format_runtime_log(backend: str, event: str, **fields: object) -> str:
    """Unified prefix for Ray/Dask driver messages (``[ray][cluster] ...``)."""
    parts = " ".join(f"{key}={value}" for key, value in fields.items())
    return f"[{backend}][{event}] {parts}".rstrip()


def format_run_start(
    backend: str,
    *,
    nodes: int,
    chunks: int,
    max_iter: int,
    init_s: float,
    seed: int | None = None,
) -> str:
    seed_part = f" seed={seed}" if seed is not None else ""
    return (
        f"[{backend}][start] nodes={nodes} chunks={chunks} "
        f"max_iter={max_iter} init_s={init_s:.2f}s{seed_part}"
    )


def log_vm_peaks(backend: str, worker_vm_peaks: dict[str, float]) -> None:
    peak_line = " ".join(
        f"{host}={rss:.0f}MB" for host, rss in sorted(worker_vm_peaks.items())
    )
    print(f"[{backend}][vm-peaks] {peak_line}")


def format_iter_log(
    iter_num: int,
    changed: int,
    total: int,
    workers: int,
    elapsed: float,
    backend: str = "lpa",
) -> str:
    pct = 100.0 * changed / total if total else 0.0
    return (
        f"[{backend}][iter={iter_num}] changed={changed}/{total} ({pct:.1f}%) "
        f"workers={workers} elapsed={elapsed:.2f}s"
    )


def format_summary_logs(
    labels: np.ndarray,
    iters: int,
    iter_times: list[float],
    converged: bool,
    backend: str = "lpa",
) -> list[str]:
    if labels.size == 0:
        sizes = [0]
        num_communities = 0
    else:
        _, counts = np.unique(labels, return_counts=True)
        sizes = sorted(int(c) for c in counts)
        num_communities = len(sizes)

    tag = backend
    p50 = sizes[len(sizes) // 2]
    p95 = sizes[min(len(sizes) - 1, int(len(sizes) * 0.95))]
    lines = [
        f"[{tag}][done] iters={iters} converged={str(converged).lower()} "
        f"communities={num_communities}",
        (
            f"[{tag}][sizes] min={sizes[0]} p50={p50} "
            f"p95={p95} max={sizes[-1]}"
        ),
    ]
    if iter_times:
        lines.append(
            f"[{tag}][timing] iter_min={min(iter_times):.2f}s "
            f"iter_median={statistics.median(iter_times):.2f}s "
            f"iter_max={max(iter_times):.2f}s total={sum(iter_times):.2f}s"
        )
    else:
        lines.append(
            f"[{tag}][timing] iter_min=0.00s iter_median=0.00s "
            f"iter_max=0.00s total=0.00s"
        )
    if not converged:
        lines.append(f"[{tag}][warn] max_iter reached, not converged")
    return lines


def count_label_changes(labels: np.ndarray, snapshot: np.ndarray) -> int:
    return int(np.count_nonzero(labels != snapshot))


def run_lpa_sequential(
    graph: Graph,
    max_iter: int = 100,
    seed: int = 42,
    log_fn: Callable[[str], None] | None = None,
) -> LpaResult:
    """Single-thread LPA reference driver."""
    n = graph.num_nodes
    if n == 0:
        return LpaResult(
            num_communities=0,
            num_levels=0,
            init_time_s=0.0,
            algorithm_time_s=0.0,
            converged=True,
            node_count=0,
            labels={},
        )

    rng = random.Random(seed)
    order = list(range(n))
    rng.shuffle(order)
    process_order = np.asarray(order, dtype=np.int64)
    labels = graph.init_labels()
    iter_times: list[float] = []
    converged = False
    snapshot = np.empty_like(labels)

    t0 = time.perf_counter()
    for i in range(1, max_iter + 1):
        t_iter = time.perf_counter()
        np.copyto(snapshot, labels)
        labels[process_order] = lpa_iteration_chunk(process_order, graph, snapshot)
        changed = count_label_changes(labels, snapshot)
        elapsed = time.perf_counter() - t_iter
        iter_times.append(elapsed)
        msg = format_iter_log(i, changed, n, 1, elapsed)
        if log_fn:
            log_fn(msg)
        else:
            print(msg)
        if changed == 0:
            converged = True
            break

    algo_time = time.perf_counter() - t0
    for line in format_summary_logs(labels, len(iter_times), iter_times, converged):
        if log_fn:
            log_fn(line)
        else:
            print(line)

    return LpaResult(
        num_communities=len(np.unique(labels)),
        num_levels=len(iter_times),
        init_time_s=0.0,
        algorithm_time_s=algo_time,
        level_times_s=iter_times,
        converged=converged,
        num_workers=1,
        node_count=n,
        partition_node_ids=graph.node_ids,
        partition_labels=labels,
        labels=graph.labels_to_dict(labels) if n <= 50_000 else {},
    )
