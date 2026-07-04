"""Benchmark run seed resolution."""

from __future__ import annotations

import os

from config import AppConfig


def resolve_benchmark_seeds(cfg: AppConfig, runs: int) -> list[int]:
    """
    Return one LPA seed per benchmark repetition.

    Default: ``cfg.seed``, ``cfg.seed + 1``, … (e.g. 42, 43, 44 for 3 runs).
    Override with env ``BENCHMARK_SEEDS=42,43,44`` (must match ``runs``).
    """
    if runs < 1:
        raise ValueError(f"runs must be >= 1, got {runs}")

    raw = os.environ.get("BENCHMARK_SEEDS", "").strip()
    if raw:
        seeds = [int(part.strip()) for part in raw.split(",") if part.strip()]
        if len(seeds) != runs:
            raise ValueError(
                f"BENCHMARK_SEEDS has {len(seeds)} values but runs={runs}"
            )
        return seeds

    return [cfg.seed + i for i in range(runs)]
