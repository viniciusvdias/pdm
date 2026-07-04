"""LPA integration: precomputed Orkut 0.1% fixture → benchmark → report.

  pytest tests/integration/test_lpa_orkut.py -m integration -v -s
  pytest tests/integration/test_lpa_orkut.py -m integration -v -s --backend ray
"""

from __future__ import annotations

import csv
import json

import pytest

from benchmark.report import generate_report
from benchmark.runner import run_benchmark_campaign
from tests.integration.dataset import (
    FIXTURE_EDGE_COUNT,
    FIXTURE_NODE_COUNT,
    IntegrationWorkspace,
)


def _assert_memory_metrics(rows: list[dict[str, str]]) -> None:
    for row in rows:
        heap_mb = float(row["peak_memory_mb"])
        driver_rss = float(row["peak_driver_rss_mb"])
        tree_rss = float(row["peak_process_tree_rss_mb"])
        cluster_rss = float(row["peak_cluster_rss_mb"])
        assert tree_rss >= driver_rss >= heap_mb
        assert tree_rss > 50.0
        assert cluster_rss >= tree_rss
        vm_peaks = json.loads(row["vm_peaks_json"])
        assert vm_peaks
        assert row["communities_json"]
        assert float(row["graph_load_time_s"]) < 5.0


@pytest.mark.integration
def test_lpa_orkut_pipeline(
    integration_workspace: IntegrationWorkspace,
    integration_approaches: list[str],
):
    ws = integration_workspace
    approaches = integration_approaches

    run_benchmark_campaign(
        ws.graph_path,
        ws.metrics_csv,
        runs=1,
        fractions=[ws.fraction_pct],
        cfg=ws.cfg,
        log_path=ws.run_log,
        run_stamp=ws.run_stamp,
        approaches=approaches,
    )
    assert ws.metrics_csv.is_file()
    assert ws.run_log.is_file()
    log_body = ws.run_log.read_text(encoding="utf-8")
    assert "dataset_slug=orkut" in log_body
    assert "orkut_0p1pct.npz" in log_body
    if "ray" in approaches:
        assert "[ray][start]" in log_body
        assert "[ray][done]" in log_body
    if "dask" in approaches:
        assert "[dask][start]" in log_body
        assert "[dask][done]" in log_body

    with ws.metrics_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == len(approaches)
    assert {r["approach"] for r in rows} == set(approaches)
    assert all(r["status"] == "success" for r in rows)
    assert all(int(r["node_count"]) == FIXTURE_NODE_COUNT for r in rows)
    assert all(int(r["num_communities"]) >= 2 for r in rows)
    _assert_memory_metrics(rows)

    for row in rows:
        communities_path = ws.benchmark_dir / row["communities_json"]
        assert communities_path.is_file()
        payload = json.loads(communities_path.read_text(encoding="utf-8"))
        assert sum(c["size"] for c in payload["clusters"]) == FIXTURE_NODE_COUNT

    generate_report(ws.metrics_csv, ws.comparison_md)
    assert ws.comparison_md.is_file()

    print(
        f"\n=== LPA 0.1% Orkut fixture ({approaches}) ===\n"
        f"  Nodes: {FIXTURE_NODE_COUNT:,}  Edges: {FIXTURE_EDGE_COUNT:,}\n"
        f"  Communities: {[int(r['num_communities']) for r in rows]}\n"
        f"  Algo (s): {[round(float(r['algorithm_time_s']), 1) for r in rows]}\n"
        f"  Load (s): {[round(float(r['graph_load_time_s']), 3) for r in rows]}"
    )


@pytest.mark.integration
def test_lpa_workers_grid(integration_workspace: IntegrationWorkspace):
    ws = integration_workspace
    run_benchmark_campaign(
        ws.graph_path,
        ws.metrics_csv,
        runs=1,
        fractions=[ws.fraction_pct],
        workers_list=[2, 4],
        cfg=ws.cfg,
        log_path=ws.run_log,
        run_stamp=ws.run_stamp,
        approaches=["ray", "dask"],
    )
    with ws.metrics_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 4
    assert {int(r["workers_requested"]) for r in rows} == {2, 4}
    assert all(r["status"] == "success" for r in rows)
