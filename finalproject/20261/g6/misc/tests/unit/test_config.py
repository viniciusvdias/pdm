"""Tests for configuration loading."""

import os
from pathlib import Path

from config import load_config, resolve_worker_count


def test_resolve_worker_count_from_env(monkeypatch):
    monkeypatch.setenv("LPA_WORKERS", "6")
    assert resolve_worker_count() == 6


def test_resolve_worker_count_from_cpu(monkeypatch):
    monkeypatch.delenv("LPA_WORKERS", raising=False)
    monkeypatch.setattr(os, "cpu_count", lambda: 8)
    assert resolve_worker_count() == 8


def test_load_config_defaults(monkeypatch):
    for key in ("RAY_NUM_CPUS", "DASK_N_WORKERS", "RAY_HEAD_ADDRESS", "DASK_SCHEDULER_ADDRESS", "LPA_WORKERS", "LPA_CHUNK_DIVISOR"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    cfg = load_config(Path("/nonexistent/config.yaml"))
    assert cfg.seed == 42
    assert cfg.lpa_max_iter == 100
    assert cfg.lpa_chunk_divisor == 4
    assert cfg.graph_directed is False
    assert cfg.reports_dir == Path("reports")
    assert cfg.dataset_slug == "orkut"
    assert cfg.dask_n_workers is None
    assert cfg.ray_head_address is None
    assert cfg.dask_scheduler_address is None


def test_load_config_chunk_divisor_override(tmp_path: Path, monkeypatch):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("lpa_chunk_divisor: 7\n", encoding="utf-8")
    monkeypatch.delenv("LPA_CHUNK_DIVISOR", raising=False)
    cfg = load_config(yaml_path)
    assert cfg.lpa_chunk_divisor == 7


def test_load_config_from_yaml(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        "seed: 7\nlpa_max_iter: 25\nreports_dir: custom/reports\n",
        encoding="utf-8",
    )
    cfg = load_config(yaml_path)
    assert cfg.seed == 7
    assert cfg.lpa_max_iter == 25
    assert cfg.reports_dir == Path("custom/reports")


def test_load_config_env_override(tmp_path: Path, monkeypatch):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("seed: 7\n", encoding="utf-8")
    monkeypatch.setenv("SEED", "99")
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    cfg = load_config(yaml_path)
    assert cfg.seed == 99
