"""Pre-build BFS-sampled graph artifacts so benchmark timing excludes SNAP scan."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from config import REPO_ROOT
from graph.graph import Graph
from preprocessing.fractions import fraction_label
from preprocessing.graph_artifact import (
    graph_artifact_paths,
    load_graph_artifact,
    save_graph_artifact,
)
from preprocessing.load_graph import load_graph_from_snap


@dataclass(frozen=True)
class FractionArtifactResult:
    artifact_path: Path
    bfs_build_time_s: float
    reused: bool


def default_artifacts_dir() -> Path:
    return REPO_ROOT / "data" / "artifacts"


def fraction_artifact_stem(dataset_slug: str, fraction_pct: float) -> str:
    return f"{dataset_slug}_{fraction_label(fraction_pct)}pct"


def fraction_artifact_path(
    dataset_slug: str,
    fraction_pct: float,
    artifacts_dir: Path | None = None,
) -> Path:
    base = (artifacts_dir or default_artifacts_dir()) / fraction_artifact_stem(
        dataset_slug, fraction_pct
    )
    npz_path, _ = graph_artifact_paths(base)
    return npz_path


def _artifact_meta_matches(
    meta_path: Path,
    *,
    dataset_slug: str,
    fraction_pct: float,
    seed: int,
    raw_path: Path,
) -> bool:
    if not meta_path.is_file():
        return False
    import json

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    return (
        meta.get("dataset_slug") == dataset_slug
        and float(meta.get("fraction_pct", -1)) == float(fraction_pct)
        and int(meta.get("seed", -1)) == seed
        and meta.get("source") == str(raw_path)
    )


def ensure_fraction_artifact(
    raw_path: Path,
    *,
    fraction_pct: float,
    seed: int,
    dataset_slug: str,
    directed: bool = False,
    artifacts_dir: Path | None = None,
    force: bool = False,
) -> FractionArtifactResult:
    """
    Build (or reuse) a compressed CSR artifact via BFS connected sampling.

    BFS + SNAP scan happen here, outside LPA timing. Benchmark loads the
    artifact with ``load_graph_artifact`` (fast npz read only).
    """
    if fraction_pct >= 100:
        raise ValueError("ensure_fraction_artifact requires fraction_pct < 100")

    raw_path = Path(raw_path)
    out_base = (artifacts_dir or default_artifacts_dir()) / fraction_artifact_stem(
        dataset_slug, fraction_pct
    )
    npz_path, meta_path = graph_artifact_paths(out_base)

    if (
        not force
        and npz_path.is_file()
        and _artifact_meta_matches(
            meta_path,
            dataset_slug=dataset_slug,
            fraction_pct=fraction_pct,
            seed=seed,
            raw_path=raw_path,
        )
    ):
        print(
            f"[benchmark] reusing BFS artifact {npz_path.name} "
            f"({fraction_pct}% seed={seed})",
            flush=True,
        )
        return FractionArtifactResult(
            artifact_path=npz_path,
            bfs_build_time_s=0.0,
            reused=True,
        )

    print(
        f"[benchmark] building BFS artifact {fraction_pct}% from {raw_path.name} "
        f"(seed={seed}) — excluded from algorithm timing ...",
        flush=True,
    )
    t0 = time.perf_counter()
    loaded = load_graph_from_snap(
        raw_path,
        fraction_pct=fraction_pct,
        seed=seed,
        directed=directed,
    )
    bfs_time = time.perf_counter() - t0
    graph: Graph = loaded.graph
    meta = {
        "dataset_slug": dataset_slug,
        "fraction_pct": fraction_pct,
        "seed": seed,
        "node_count": loaded.node_count,
        "edge_count": loaded.edge_count,
        "source": str(raw_path),
        "bfs_build_time_s": bfs_time,
        "graph_directed": directed,
    }
    save_graph_artifact(graph, out_base, meta=meta)
    print(
        f"[benchmark] BFS artifact ready: {loaded.node_count:,} nodes, "
        f"{loaded.edge_count:,} edges in {bfs_time:.1f}s → {npz_path.name}",
        flush=True,
    )
    return FractionArtifactResult(
        artifact_path=npz_path,
        bfs_build_time_s=bfs_time,
        reused=False,
    )


def load_fraction_for_benchmark(
    path: Path,
    *,
    fraction_pct: float,
    seed: int,
    dataset_slug: str,
    directed: bool = False,
    artifacts_dir: Path | None = None,
):
    """
    Load graph for benchmark: pre-built BFS artifact for partial fractions,
    direct SNAP load for 100%.
    """
    path = Path(path)
    if is_graph_artifact_path(path):
        from preprocessing.graph_artifact import is_graph_artifact

        if is_graph_artifact(path):
            print(f"[benchmark] loading fixture {path} ...", flush=True)
            return load_graph_artifact(path)

    if fraction_pct >= 100:
        print(f"[benchmark] loading SNAP 100% from {path} ...", flush=True)
        return load_graph_from_snap(
            path,
            fraction_pct=100.0,
            seed=seed,
            directed=directed,
        )

    artifact = ensure_fraction_artifact(
        path,
        fraction_pct=fraction_pct,
        seed=seed,
        dataset_slug=dataset_slug,
        directed=directed,
        artifacts_dir=artifacts_dir,
    )
    loaded = load_graph_artifact(artifact.artifact_path)
    return loaded


def is_graph_artifact_path(path: Path) -> bool:
    from preprocessing.graph_artifact import is_graph_artifact

    return is_graph_artifact(path)
