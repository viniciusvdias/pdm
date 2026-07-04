"""Benchmarking and reporting."""

from benchmark.report import generate_report

__all__ = ["generate_report", "run_benchmark_campaign"]


def run_benchmark_campaign(*args, **kwargs):
    from benchmark.runner import run_benchmark_campaign as _run

    return _run(*args, **kwargs)
