"""Load configuration from YAML and environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class AppConfig:
    graph_raw_path: Path
    dataset_slug: str
    reports_dir: Path
    seed: int
    lpa_max_iter: int
    lpa_chunk_divisor: int
    graph_directed: bool
    ray_num_cpus: int | None
    dask_n_workers: int | None
    ray_head_address: str | None
    dask_scheduler_address: str | None


def resolve_worker_count() -> int:
    """Parallel workers / LPA chunks from ``LPA_WORKERS`` or host CPU count."""
    raw = os.environ.get("LPA_WORKERS", "").strip()
    if raw:
        return max(1, int(raw))
    return max(1, os.cpu_count() or 1)


def effective_ray_cpus(cfg: AppConfig) -> int | None:
    if cfg.ray_head_address:
        return cfg.ray_num_cpus
    return cfg.ray_num_cpus if cfg.ray_num_cpus is not None else cfg.lpa_chunk_divisor


def effective_dask_workers(cfg: AppConfig) -> int | None:
    if cfg.dask_scheduler_address:
        return cfg.dask_n_workers
    return cfg.dask_n_workers if cfg.dask_n_workers is not None else cfg.lpa_chunk_divisor


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _optional_int(value: Any) -> int | None:
    if value is None or value == "" or value == "null":
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value is None or value == "" or value == "null":
        return None
    return str(value)


def _bool(value: Any, default: bool) -> bool:
    if value is None or value == "" or value == "null":
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes", "on")


def _int_or_workers(value: Any) -> int:
    if value is None or value == "" or value == "null":
        return resolve_worker_count()
    return int(value)


# Worker chain: resolve_worker_count() → lpa_chunk_divisor (when null in YAML)
# → effective_ray_cpus / effective_dask_workers (when ray_num_cpus / dask_n_workers null).


def load_config(config_path: Path | None = None) -> AppConfig:
    path = config_path or REPO_ROOT / "config.yaml"
    if not path.is_file():
        path = REPO_ROOT / "config.yaml.example"
    raw = _load_yaml(path)

    def get(key: str, default: Any) -> Any:
        env_key = key.upper()
        if env_key in os.environ:
            return os.environ[env_key]
        alt = {
            "graph_raw_path": "GRAPH_RAW_PATH",
            "dataset_slug": "DATASET_SLUG",
            "reports_dir": "REPORTS_DIR",
            "seed": "SEED",
            "lpa_max_iter": "LPA_MAX_ITER",
            "lpa_chunk_divisor": "LPA_CHUNK_DIVISOR",
            "graph_directed": "GRAPH_DIRECTED",
            "ray_num_cpus": "RAY_NUM_CPUS",
            "dask_n_workers": "DASK_N_WORKERS",
            "ray_head_address": "RAY_HEAD_ADDRESS",
            "dask_scheduler_address": "DASK_SCHEDULER_ADDRESS",
        }.get(key)
        if alt and alt in os.environ:
            return os.environ[alt]
        return raw.get(key, default)

    return AppConfig(
        graph_raw_path=Path(get("graph_raw_path", "data/raw/soc-orkut-relationships.txt")),
        dataset_slug=str(get("dataset_slug", "orkut")),
        reports_dir=Path(get("reports_dir", "reports")),
        seed=int(get("seed", 42)),
        lpa_max_iter=int(get("lpa_max_iter", 100)),
        lpa_chunk_divisor=_int_or_workers(get("lpa_chunk_divisor", None)),
        graph_directed=_bool(get("graph_directed", False), default=False),
        ray_num_cpus=_optional_int(get("ray_num_cpus", None)),
        dask_n_workers=_optional_int(get("dask_n_workers", None)),
        ray_head_address=_optional_str(get("ray_head_address", None)),
        dask_scheduler_address=_optional_str(get("dask_scheduler_address", None)),
    )
