"""Tests for 100% memory/time extrapolation."""

from benchmark.memory_estimate import (
    POKEC_100_EDGE_COUNT,
    POKEC_100_NODE_COUNT,
    estimate_csr_bytes,
    estimate_for_fraction,
    extrapolate_time_to_100,
    format_estimate_report,
)


def test_estimate_csr_grows_with_edges():
    small = estimate_csr_bytes(3_072, 12_000)
    large = estimate_csr_bytes(163_280, 1_880_000)
    full = estimate_csr_bytes(POKEC_100_NODE_COUNT, POKEC_100_EDGE_COUNT)
    assert small.csr_steady_mb < large.csr_steady_mb < full.csr_steady_mb
    assert full.load_peak_mb > full.csr_steady_mb


def test_extrapolate_time_to_100_between_linear_and_edges():
    t = extrapolate_time_to_100()
    assert t.ray_algo_s_low < t.ray_algo_s_high
    linear_at_100 = 84.57 * (POKEC_100_EDGE_COUNT / 1_880_997)
    assert t.ray_algo_s_low < linear_at_100
    assert t.ray_algo_s_high >= linear_at_100 * 0.95


def test_estimate_for_fraction_100_report():
    est = estimate_for_fraction(100, POKEC_100_NODE_COUNT, POKEC_100_EDGE_COUNT)
    text = format_estimate_report(est)
    assert "Orkut" in text
    assert "Process tree Ray" in text
    assert est.cluster.process_tree_ray_mb > est.cluster.driver_ray_mb
