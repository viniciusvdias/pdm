"""Timestamped paths for benchmark CSV and comparison reports."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

STAMP_PATTERN = re.compile(r"^metrics_raw_(?P<stamp>\d{8}T\d{6})\.csv$")
LATEST_RUN_FILE = "latest_run.txt"


def benchmark_run_stamp(when: datetime | None = None) -> str:
    """Local start-time stamp for a benchmark run (filesystem-safe)."""
    moment = when or datetime.now()
    return moment.strftime("%Y%m%dT%H%M%S")


def partitions_dir(directory: Path, stamp: str) -> Path:
    return directory / f"partitions_{stamp}"


def metrics_csv_path(directory: Path, stamp: str) -> Path:
    return directory / f"metrics_raw_{stamp}.csv"


def comparison_md_path(directory: Path, stamp: str) -> Path:
    return directory / f"comparison_{stamp}.md"


def run_log_path(directory: Path, stamp: str) -> Path:
    return directory / f"benchmark_run_{stamp}.log"


def stamp_from_metrics_csv(path: Path) -> str | None:
    match = STAMP_PATTERN.match(path.name)
    return match.group("stamp") if match else None


def write_run_stamp(directory: Path, stamp: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    marker = directory / LATEST_RUN_FILE
    marker.write_text(f"{stamp}\n", encoding="utf-8")
    return marker


def read_run_stamp(directory: Path) -> str | None:
    marker = directory / LATEST_RUN_FILE
    if not marker.is_file():
        return None
    text = marker.read_text(encoding="utf-8").strip()
    return text or None


def latest_metrics_csv(directory: Path) -> Path:
    candidates = sorted(
        directory.glob("metrics_raw_*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        msg = f"No metrics_raw_*.csv found in {directory}"
        raise FileNotFoundError(msg)
    return candidates[0]


def resolve_report_paths(
    reports_dir: Path,
    input_csv: Path | None = None,
    output_md: Path | None = None,
) -> tuple[Path, Path]:
    """Resolve CSV input and MD output, pairing by run stamp when omitted."""
    csv_path = input_csv or latest_metrics_csv(reports_dir)
    if output_md is not None:
        return csv_path, output_md
    stamp = stamp_from_metrics_csv(csv_path) or read_run_stamp(reports_dir)
    if stamp is None:
        msg = f"Cannot infer run stamp from {csv_path}; pass --output-md explicitly"
        raise ValueError(msg)
    return csv_path, comparison_md_path(reports_dir, stamp)
