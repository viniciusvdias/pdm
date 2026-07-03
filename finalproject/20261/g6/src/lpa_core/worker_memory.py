"""Peak RSS sampling inside Ray/Dask worker processes."""

from __future__ import annotations

import socket

import psutil

_peak_rss_mb: float = 0.0


def worker_host_id() -> str:
    """Stable host label for VM mapping (hostname)."""
    return socket.gethostname()


def sample_worker_peak() -> float:
    """Update and return peak RSS (MB) for the current worker process."""
    global _peak_rss_mb
    rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
    _peak_rss_mb = max(_peak_rss_mb, rss_mb)
    return _peak_rss_mb


def merge_worker_samples(
    peaks: dict[str, float],
    samples: list[dict[str, float | str]],
) -> None:
    """In-place max merge of worker task samples into ``peaks``."""
    for sample in samples:
        host = str(sample["host"])
        rss = float(sample["peak_rss_mb"])
        peaks[host] = max(peaks.get(host, 0.0), rss)
