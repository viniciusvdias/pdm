"""Integration test fixtures and CLI options."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmark.paths import benchmark_run_stamp, write_run_stamp
from config import AppConfig
from tests.integration.dataset import (
    DATASET_SLUG,
    FIXTURE_PATH,
    FRACTION_PCT,
    OUTPUT_DIR,
    SEED,
    IntegrationWorkspace,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--backend",
        action="store",
        default="both",
        choices=["both", "ray", "dask"],
        help="LPA runtime: both (default), ray, or dask",
    )


def _approaches_for_backend(backend: str) -> list[str]:
    if backend == "ray":
        return ["ray"]
    if backend == "dask":
        return ["dask"]
    return ["ray", "dask"]


@pytest.fixture
def integration_approaches(request: pytest.FixtureRequest) -> list[str]:
    return _approaches_for_backend(request.config.getoption("--backend"))


@pytest.fixture
def integration_graph_path() -> Path:
    if not FIXTURE_PATH.is_file():
        pytest.skip(
            f"Missing {FIXTURE_PATH} — run: bash scripts/build_integration_fixture.sh"
        )
    return FIXTURE_PATH


@pytest.fixture
def integration_workspace(integration_graph_path: Path) -> IntegrationWorkspace:
    run_stamp = benchmark_run_stamp()
    benchmark_dir = OUTPUT_DIR / "benchmark"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    write_run_stamp(benchmark_dir, run_stamp)

    cfg = AppConfig(
        graph_raw_path=integration_graph_path,
        dataset_slug=DATASET_SLUG,
        reports_dir=benchmark_dir,
        seed=SEED,
        lpa_max_iter=100,
        lpa_chunk_divisor=12,
        graph_directed=False,
        ray_num_cpus=3,
        dask_n_workers=3,
        ray_head_address=None,
        dask_scheduler_address=None,
    )
    return IntegrationWorkspace(
        root=OUTPUT_DIR,
        benchmark_dir=benchmark_dir,
        cfg=cfg,
        graph_path=integration_graph_path,
        fraction_pct=FRACTION_PCT,
        run_stamp=run_stamp,
    )
