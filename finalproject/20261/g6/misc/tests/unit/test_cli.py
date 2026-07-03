"""CLI smoke tests with mocked heavy backends."""

from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock

import cli.main as cli_main
from benchmark.paths import write_run_stamp
from cli.main import cmd_benchmark, cmd_lpa_dask, cmd_lpa_ray, cmd_report, main
from config import AppConfig
from graph.graph import Graph
from lpa_core.lpa import LpaResult
from preprocessing.load_graph import GraphLoadResult


def _fake_loaded(graph: Graph | None = None) -> GraphLoadResult:
    g = graph or Graph.from_undirected_edges([(0, 1)])
    return GraphLoadResult(
        graph=g,
        load_time_s=0.05,
        node_count=g.num_nodes,
        edge_count=1,
        fraction_pct=100.0,
    )


def test_main_lpa_ray_missing_input(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    code = main(["lpa-ray", "--input", str(tmp_path / "nope.txt")])
    assert code == 1


def test_cmd_lpa_ray_json_output(tmp_path: Path, capsys, monkeypatch):
    raw = tmp_path / "raw.txt"
    raw.write_text("0 1\n", encoding="utf-8")
    fake = LpaResult(
        num_communities=1,
        num_levels=3,
        init_time_s=0.01,
        algorithm_time_s=0.1,
    )
    monkeypatch.setattr(cli_main, "load_graph", lambda *a, **k: _fake_loaded())
    monkeypatch.setattr(cli_main, "run_lpa_ray", lambda *a, **k: fake)
    args = Namespace(
        input=str(raw),
        fraction=None,
        seed=None,
        max_iter=None,
        num_cpus=None,
        output_partition=None,
    )
    assert cmd_lpa_ray(args) == 0
    assert '"num_communities": 1' in capsys.readouterr().out


def test_cmd_lpa_dask_json_output(tmp_path: Path, capsys, monkeypatch):
    raw = tmp_path / "raw.txt"
    raw.write_text("0 1\n", encoding="utf-8")
    fake = LpaResult(
        num_communities=2,
        num_levels=2,
        init_time_s=0.02,
        algorithm_time_s=0.2,
    )
    monkeypatch.setattr(cli_main, "load_graph", lambda *a, **k: _fake_loaded())
    monkeypatch.setattr("dask_impl.lpa_dask.run_lpa_dask", lambda *a, **k: fake)
    args = Namespace(
        input=str(raw),
        fraction=None,
        seed=None,
        max_iter=None,
        n_workers=None,
        output_partition=None,
    )
    assert cmd_lpa_dask(args) == 0
    assert '"num_communities": 2' in capsys.readouterr().out


def test_cmd_report_and_benchmark(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "m.csv"
    csv_path.write_text("approach,fraction_pct\n", encoding="utf-8")
    md_path = tmp_path / "r.md"
    monkeypatch.setattr(
        cli_main,
        "generate_report",
        lambda inp, out: out.write_text("# ok\n") or out,
    )
    assert cmd_report(Namespace(input_csv=str(csv_path), output_md=str(md_path))) == 0

    raw = tmp_path / "raw.txt"
    raw.write_text("0 1\n", encoding="utf-8")
    out_csv = tmp_path / "bench.csv"
    monkeypatch.setattr(
        "benchmark.runner.run_benchmark_campaign",
        lambda *a, **k: out_csv,
    )
    assert (
        cmd_benchmark(
            Namespace(
                input=str(raw),
                output_csv=str(out_csv),
                runs=1,
                fractions="100",
                fraction=None,
                seed=None,
                workers="",
                ray_only=False,
                dask_only=False,
                run_stamp=None,
                append=False,
            )
        )
        == 0
    )


def test_cmd_benchmark_append_reuses_latest_stamp(tmp_path: Path, monkeypatch):
    raw = tmp_path / "raw.txt"
    raw.write_text("0 1\n", encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    write_run_stamp(reports, "20260101T120000")
    captured: dict[str, str] = {}

    def _fake_campaign(_path, out, **kwargs):
        captured["out"] = str(out)
        captured["stamp"] = kwargs.get("run_stamp")
        return out

    monkeypatch.setattr(cli_main, "load_config", lambda: AppConfig(
        graph_raw_path=raw,
        dataset_slug="orkut",
        reports_dir=reports,
        seed=42,
        lpa_max_iter=50,
        lpa_chunk_divisor=2,
        graph_directed=False,
        ray_num_cpus=None,
        dask_n_workers=None,
        ray_head_address=None,
        dask_scheduler_address=None,
    ))
    monkeypatch.setattr("benchmark.runner.run_benchmark_campaign", _fake_campaign)
    code = cmd_benchmark(
        Namespace(
            input=str(raw),
            output_csv=None,
            runs=1,
            fractions="100",
            fraction=None,
            seed=None,
            workers="",
            ray_only=False,
            dask_only=True,
            run_stamp=None,
            append=True,
        )
    )
    assert code == 0
    assert captured["stamp"] == "20260101T120000"
    assert captured["out"].endswith("metrics_raw_20260101T120000.csv")


def test_parse_workers_valid():
    from cli.main import _parse_workers

    assert _parse_workers("2,4,6") == [2, 4, 6]


def test_parse_workers_empty():
    from cli.main import _parse_workers

    assert _parse_workers("") == []


def test_cmd_benchmark_passes_workers_list(tmp_path: Path, monkeypatch):
    raw = tmp_path / "raw.txt"
    raw.write_text("0 1\n", encoding="utf-8")
    captured: dict = {}

    def _fake_campaign(_path, out, **kwargs):
        captured["workers_list"] = kwargs.get("workers_list")
        return out

    monkeypatch.setattr("benchmark.runner.run_benchmark_campaign", _fake_campaign)
    code = cmd_benchmark(
        Namespace(
            input=str(raw),
            output_csv=str(tmp_path / "m.csv"),
            runs=1,
            fractions="100",
            fraction=None,
            seed=None,
            workers="2,4",
            ray_only=False,
            dask_only=False,
            run_stamp=None,
            append=False,
        )
    )
    assert code == 0
    assert captured["workers_list"] == [2, 4]


def test_cmd_benchmark_rejects_both_flags(tmp_path: Path, capsys):
    raw = tmp_path / "raw.txt"
    raw.write_text("0 1\n", encoding="utf-8")
    code = cmd_benchmark(
        Namespace(
            input=str(raw),
            output_csv=None,
            runs=1,
            fractions="100",
            fraction=None,
            seed=None,
            workers="",
            ray_only=True,
            dask_only=True,
            run_stamp=None,
            append=False,
        )
    )
    assert code == 1
    assert "only one" in capsys.readouterr().err.lower()
