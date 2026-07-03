"""Tests for timestamped benchmark report paths."""

from datetime import datetime
from pathlib import Path

import pytest

from benchmark.paths import (
    benchmark_run_stamp,
    comparison_md_path,
    latest_metrics_csv,
    metrics_csv_path,
    resolve_report_paths,
    stamp_from_metrics_csv,
    write_run_stamp,
)


def test_benchmark_run_stamp_format():
    stamp = benchmark_run_stamp(datetime(2026, 6, 13, 20, 53, 42))
    assert stamp == "20260613T205342"


def test_metrics_and_comparison_paths(tmp_path: Path):
    stamp = "20260613T205342"
    assert metrics_csv_path(tmp_path, stamp).name == "metrics_raw_20260613T205342.csv"
    assert comparison_md_path(tmp_path, stamp).name == "comparison_20260613T205342.md"
    assert stamp_from_metrics_csv(metrics_csv_path(tmp_path, stamp)) == stamp


def test_resolve_report_paths_pairs_stamp(tmp_path: Path):
    stamp = "20260613T205342"
    csv_path = metrics_csv_path(tmp_path, stamp)
    csv_path.write_text("approach\n", encoding="utf-8")
    write_run_stamp(tmp_path, stamp)
    inp, out = resolve_report_paths(tmp_path)
    assert inp == csv_path
    assert out == comparison_md_path(tmp_path, stamp)


def test_latest_metrics_csv(tmp_path: Path):
    older = metrics_csv_path(tmp_path, "20260101T000000")
    newer = metrics_csv_path(tmp_path, "20260201T000000")
    older.write_text("a\n", encoding="utf-8")
    newer.write_text("b\n", encoding="utf-8")
    newer.touch()
    assert latest_metrics_csv(tmp_path) == newer


def test_latest_metrics_csv_missing(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        latest_metrics_csv(tmp_path)
