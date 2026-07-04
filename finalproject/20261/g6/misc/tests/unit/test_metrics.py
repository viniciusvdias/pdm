"""Tests for benchmark metrics helpers."""

import psutil

from benchmark.metrics import track_memory_peaks, throughput_nodes_per_s


def test_throughput_nodes_per_s():
    assert throughput_nodes_per_s(1000, 2.0) == 500.0
    assert throughput_nodes_per_s(100, 0.0) == 0.0


def test_track_memory_peaks_includes_process_tree():
    with track_memory_peaks(poll_interval_s=0.05) as peaks:
        _ = [0] * 100_000
    assert peaks.tracemalloc_mb >= 0.0
    assert peaks.driver_rss_mb >= _rss_mb(psutil.Process()) * 0.5
    assert peaks.process_tree_rss_mb >= peaks.driver_rss_mb


def _rss_mb(proc: psutil.Process) -> float:
    return proc.memory_info().rss / (1024 * 1024)
