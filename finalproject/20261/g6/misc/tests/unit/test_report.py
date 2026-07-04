"""Tests for benchmark Markdown report generation."""

import csv
from pathlib import Path

from benchmark.report import generate_report


def _write_metrics_csv(path: Path) -> None:
    rows = [
        {
            "approach": "ray",
            "fraction_pct": "10",
            "run_index": "1",
            "init_time_s": "0.1",
            "algorithm_time_s": "1.0",
            "peak_memory_mb": "50",
            "peak_driver_rss_mb": "80",
            "peak_process_tree_rss_mb": "512",
            "throughput_nodes_per_s": "100",
            "modularity_q": "0.42",
            "num_communities": "3",
            "num_levels": "2",
            "max_iter": "50",
            "seed": "42",
            "converged": "True",
            "status": "success",
            "error_message": "",
            "level_times_json": "[0.5]",
        },
        {
            "approach": "dask",
            "fraction_pct": "10",
            "run_index": "1",
            "init_time_s": "2.0",
            "algorithm_time_s": "3.0",
            "peak_memory_mb": "0",
            "peak_driver_rss_mb": "0",
            "peak_process_tree_rss_mb": "0",
            "throughput_nodes_per_s": "33",
            "modularity_q": "0.40",
            "num_communities": "4",
            "num_levels": "2",
            "max_iter": "50",
            "seed": "42",
            "converged": "True",
            "status": "failed",
            "error_message": "worker crash",
            "level_times_json": "[]",
        },
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def test_generate_report(tmp_path: Path):
    csv_path = tmp_path / "metrics.csv"
    md_path = tmp_path / "report.md"
    _write_metrics_csv(csv_path)
    out = generate_report(csv_path, md_path)
    text = out.read_text(encoding="utf-8")
    assert "Ray vs Dask" in text
    assert "ray" in text
    assert "Execuções falhadas" in text
    assert "worker crash" in text


def test_report_scalability_section(tmp_path: Path):
    csv_path = tmp_path / "metrics_scalability.csv"
    rows = [
        {
            "approach": "ray",
            "fraction_pct": "1",
            "run_index": "1",
            "algorithm_time_s": "1.0",
            "peak_process_tree_rss_mb": "1024",
            "throughput_nodes_per_s": "1000",
            "num_communities": "3",
            "status": "success",
            "workers_requested": "2",
        },
        {
            "approach": "ray",
            "fraction_pct": "1",
            "run_index": "1",
            "algorithm_time_s": "0.5",
            "peak_process_tree_rss_mb": "2048",
            "throughput_nodes_per_s": "2000",
            "num_communities": "3",
            "status": "success",
            "workers_requested": "4",
        },
        {
            "approach": "dask",
            "fraction_pct": "10",
            "run_index": "1",
            "algorithm_time_s": "2.0",
            "peak_process_tree_rss_mb": "4096",
            "throughput_nodes_per_s": "500",
            "num_communities": "4",
            "status": "success",
            "workers_requested": "2",
        },
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    md_path = tmp_path / "report_scalability.md"
    text = generate_report(csv_path, md_path).read_text(encoding="utf-8")
    assert "Escalabilidade" in text
    assert "2000.0" in text
    assert "seeds e partições LPA" in text
