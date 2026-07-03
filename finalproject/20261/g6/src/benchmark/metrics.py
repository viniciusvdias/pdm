"""Memory and throughput metrics helpers."""

from __future__ import annotations

import threading
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import psutil


@dataclass
class MemoryPeaks:
    """Peak memory readings for a benchmark run."""

    tracemalloc_mb: float = 0.0
    driver_rss_mb: float = 0.0
    process_tree_rss_mb: float = 0.0


def _rss_mb(proc: psutil.Process) -> float:
    return proc.memory_info().rss / (1024 * 1024)


def _process_tree_rss_mb(root: psutil.Process) -> tuple[float, float]:
    """Return (driver_rss_mb, total_rss_mb) for root and all descendants."""
    driver = _rss_mb(root)
    total = driver
    for child in root.children(recursive=True):
        try:
            total += _rss_mb(child)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return driver, total


@contextmanager
def track_memory_peaks(
    poll_interval_s: float = 0.25,
) -> Iterator[MemoryPeaks]:
    """Track tracemalloc heap and RSS peaks (driver + worker child processes)."""
    peaks = MemoryPeaks()
    stop = threading.Event()
    root = psutil.Process()

    def poll_rss() -> None:
        while not stop.is_set():
            try:
                driver, total = _process_tree_rss_mb(root)
            except psutil.NoSuchProcess:
                break
            peaks.driver_rss_mb = max(peaks.driver_rss_mb, driver)
            peaks.process_tree_rss_mb = max(peaks.process_tree_rss_mb, total)
            stop.wait(poll_interval_s)

    tracemalloc.start()
    poller = threading.Thread(target=poll_rss, daemon=True)
    poller.start()
    try:
        yield peaks
    finally:
        stop.set()
        poller.join(timeout=2.0)
        try:
            driver, total = _process_tree_rss_mb(root)
            peaks.driver_rss_mb = max(peaks.driver_rss_mb, driver)
            peaks.process_tree_rss_mb = max(peaks.process_tree_rss_mb, total)
        except psutil.NoSuchProcess:
            pass
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peaks.tracemalloc_mb = peak_bytes / (1024 * 1024)


def throughput_nodes_per_s(node_count: int, algorithm_time_s: float) -> float:
    if algorithm_time_s <= 0:
        return 0.0
    return node_count / algorithm_time_s
