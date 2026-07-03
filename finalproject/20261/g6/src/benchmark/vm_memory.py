"""Merge driver + worker RSS peaks into per-VM and cluster totals."""

from __future__ import annotations

__all__ = [
    "merge_vm_memory_peaks",
    "peak_cluster_rss_mb",
]


def merge_vm_memory_peaks(
    *,
    driver_host: str,
    driver_peak_mb: float,
    worker_peaks_mb: dict[str, float],
    process_tree_mb: float,
) -> dict[str, float]:
    """
    Per-VM peak RSS (MB).

    Each host keeps the max RSS seen on that machine. On a single host (local
    dev), ``process_tree_mb`` is used when it exceeds the per-host max so local
    child workers are not under-counted.
    """
    merged = dict(worker_peaks_mb)
    merged[driver_host] = max(merged.get(driver_host, 0.0), driver_peak_mb)
    if len(merged) <= 1:
        only_host = driver_host if driver_host in merged else next(iter(merged))
        merged[only_host] = max(merged[only_host], process_tree_mb)
    return merged


def peak_cluster_rss_mb(vm_peaks: dict[str, float]) -> float:
    """Sum of per-VM peaks (approximate cluster RAM high-water mark)."""
    return sum(vm_peaks.values())
