"""Per-VM memory peak aggregation tests."""

from lpa_core.worker_memory import merge_worker_samples
from benchmark.vm_memory import merge_vm_memory_peaks, peak_cluster_rss_mb


def test_merge_worker_samples_keeps_max_per_host():
    peaks: dict[str, float] = {}
    merge_worker_samples(
        peaks,
        [
            {"host": "vm2", "peak_rss_mb": 100.0},
            {"host": "vm2", "peak_rss_mb": 250.0},
            {"host": "vm3", "peak_rss_mb": 180.0},
        ],
    )
    assert peaks == {"vm2": 250.0, "vm3": 180.0}


def test_merge_vm_memory_peaks_sums_cluster_on_multi_host():
    vm = merge_vm_memory_peaks(
        driver_host="vm1",
        driver_peak_mb=600.0,
        worker_peaks_mb={"vm2": 400.0, "vm3": 350.0},
        process_tree_mb=900.0,
    )
    assert vm == {"vm1": 600.0, "vm2": 400.0, "vm3": 350.0}
    assert peak_cluster_rss_mb(vm) == 1350.0


def test_merge_vm_memory_peaks_uses_process_tree_on_single_host():
    vm = merge_vm_memory_peaks(
        driver_host="localhost",
        driver_peak_mb=500.0,
        worker_peaks_mb={"localhost": 450.0},
        process_tree_mb=1400.0,
    )
    assert vm["localhost"] == 1400.0
