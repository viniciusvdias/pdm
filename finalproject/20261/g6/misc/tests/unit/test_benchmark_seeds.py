"""Benchmark seed resolution tests."""

import pytest

from benchmark.seeds import resolve_benchmark_seeds
from config import AppConfig


def _cfg(seed: int = 42) -> AppConfig:
    return AppConfig(
        graph_raw_path=__import__("pathlib").Path("data/raw/x.txt"),
        dataset_slug="orkut",
        reports_dir=__import__("pathlib").Path("reports"),
        seed=seed,
        lpa_max_iter=50,
        lpa_chunk_divisor=3,
        graph_directed=False,
        ray_num_cpus=None,
        dask_n_workers=None,
        ray_head_address=None,
        dask_scheduler_address=None,
    )


def test_resolve_benchmark_seeds_default_increment(monkeypatch):
    monkeypatch.delenv("BENCHMARK_SEEDS", raising=False)
    assert resolve_benchmark_seeds(_cfg(42), 3) == [42, 43, 44]


def test_resolve_benchmark_seeds_explicit_env(monkeypatch):
    monkeypatch.setenv("BENCHMARK_SEEDS", "7,8,9")
    assert resolve_benchmark_seeds(_cfg(42), 3) == [7, 8, 9]


def test_resolve_benchmark_seeds_env_length_mismatch(monkeypatch):
    monkeypatch.setenv("BENCHMARK_SEEDS", "1,2")
    with pytest.raises(ValueError, match="BENCHMARK_SEEDS"):
        resolve_benchmark_seeds(_cfg(), 3)
