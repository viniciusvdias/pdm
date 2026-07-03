"""Benchmark orchestration: runs per approach × fraction × workers."""

from __future__ import annotations

import csv
import json
import socket
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import ray

from benchmark.memory_estimate import estimate_for_fraction, format_estimate_report
from benchmark.seeds import resolve_benchmark_seeds
from benchmark.metrics import track_memory_peaks, throughput_nodes_per_s
from benchmark.vm_memory import merge_vm_memory_peaks, peak_cluster_rss_mb
from benchmark.partitions import partition_stem, write_partition
from benchmark.paths import partitions_dir
from benchmark.run_log import (
    approach_log_capture,
    close_benchmark_log,
    open_benchmark_log,
    write_log_header,
    write_log_section,
    write_log_summary,
)
from config import AppConfig, effective_dask_workers, effective_ray_cpus, load_config
from graph.graph import Graph
from preprocessing.fraction_artifacts import load_fraction_for_benchmark
from preprocessing.graph_artifact import is_graph_artifact
from ray_impl.lpa_ray import run_lpa_ray

CSV_HEADER = [
    "approach",
    "fraction_pct",
    "run_index",
    "node_count",
    "graph_load_time_s",
    "init_time_s",
    "algorithm_time_s",
    "total_time_s",
    "peak_memory_mb",
    "peak_driver_rss_mb",
    "peak_process_tree_rss_mb",
    "throughput_nodes_per_s",
    "num_communities",
    "num_levels",
    "max_iter",
    "seed",
    "converged",
    "status",
    "error_message",
    "level_times_json",
    "partition_summary",
    "communities_json",
    "vm_peaks_json",
    "peak_cluster_rss_mb",
    "workers_requested",
    "workers_actual",
]

DEFAULT_APPROACHES = ("ray", "dask")


@dataclass
class BenchmarkRow:
    approach: str
    fraction_pct: float
    run_index: int
    node_count: int
    graph_load_time_s: float
    init_time_s: float
    algorithm_time_s: float
    total_time_s: float
    peak_memory_mb: float
    peak_driver_rss_mb: float
    peak_process_tree_rss_mb: float
    throughput_nodes_per_s: float
    num_communities: int
    num_levels: int
    max_iter: float
    seed: int
    converged: bool
    status: str
    error_message: str
    level_times_json: str
    partition_summary: str
    communities_json: str
    vm_peaks_json: str
    peak_cluster_rss_mb: float
    workers_requested: int = 0
    workers_actual: int = 0


def _shutdown_ray() -> None:
    if ray.is_initialized():
        ray.shutdown()


def _save_partition(
    reports_dir: Path,
    run_stamp: str | None,
    approach: str,
    fraction_pct: float,
    cfg: AppConfig,
    run_index: int,
    res,
    mem,
) -> tuple[str, str, dict[str, float], float]:
    arrays = res.partition_arrays()
    if arrays is None or run_stamp is None:
        return "", "", {}, 0.0
    node_ids, labels_arr = arrays
    out_dir = partitions_dir(reports_dir, run_stamp)
    stem = partition_stem(approach, cfg.dataset_slug, fraction_pct, run_index)
    vm_peaks = merge_vm_memory_peaks(
        driver_host=socket.gethostname(),
        driver_peak_mb=mem.driver_rss_mb,
        worker_peaks_mb=getattr(res, "worker_vm_peaks_mb", {}) or {},
        process_tree_mb=mem.process_tree_rss_mb,
    )
    cluster_peak = peak_cluster_rss_mb(vm_peaks)
    summary = {
        "approach": approach,
        "dataset_slug": cfg.dataset_slug,
        "fraction_pct": fraction_pct,
        "run_index": run_index,
        "seed": cfg.seed,
        "num_communities": res.num_communities,
        "num_levels": res.num_levels,
        "converged": res.converged,
        "init_time_s": res.init_time_s,
        "algorithm_time_s": res.algorithm_time_s,
        "total_time_s": res.total_time_s,
        "peak_memory_mb": mem.tracemalloc_mb,
        "peak_driver_rss_mb": mem.driver_rss_mb,
        "peak_process_tree_rss_mb": mem.process_tree_rss_mb,
        "vm_memory_peaks_mb": vm_peaks,
        "peak_cluster_rss_mb": cluster_peak,
        "throughput_nodes_per_s": throughput_nodes_per_s(
            res.node_count, res.algorithm_time_s
        ),
    }
    summary_path, communities_path = write_partition(
        out_dir, stem, node_ids, labels_arr, summary
    )
    return (
        str(summary_path.relative_to(reports_dir)),
        str(communities_path.relative_to(reports_dir)),
        vm_peaks,
        cluster_peak,
    )


def _row_from_result(
    approach: str,
    fraction_pct: float,
    cfg: AppConfig,
    mem,
    res,
    run_index: int,
    reports_dir: Path,
    run_stamp: str | None,
    graph_load_time_s: float,
    *,
    workers_requested: int,
) -> BenchmarkRow:
    part_summary, communities_json, vm_peaks, cluster_peak = _save_partition(
        reports_dir,
        run_stamp,
        approach,
        fraction_pct,
        cfg,
        run_index,
        res,
        mem,
    )
    return BenchmarkRow(
        approach=approach,
        fraction_pct=fraction_pct,
        run_index=run_index,
        node_count=res.node_count,
        graph_load_time_s=graph_load_time_s,
        init_time_s=res.init_time_s,
        algorithm_time_s=res.algorithm_time_s,
        total_time_s=res.total_time_s + graph_load_time_s,
        peak_memory_mb=mem.tracemalloc_mb,
        peak_driver_rss_mb=mem.driver_rss_mb,
        peak_process_tree_rss_mb=mem.process_tree_rss_mb,
        throughput_nodes_per_s=throughput_nodes_per_s(
            res.node_count, res.algorithm_time_s
        ),
        num_communities=res.num_communities,
        num_levels=res.num_levels,
        max_iter=float(cfg.lpa_max_iter),
        seed=cfg.seed,
        converged=res.converged,
        status="success",
        error_message="",
        level_times_json=json.dumps(res.level_times_s),
        partition_summary=part_summary,
        communities_json=communities_json,
        vm_peaks_json=json.dumps(vm_peaks),
        peak_cluster_rss_mb=cluster_peak,
        workers_requested=workers_requested,
        workers_actual=res.num_workers,
    )


def _run_approach(
    approach: str,
    graph: Graph,
    fraction_pct: float,
    graph_load_time_s: float,
    cfg: AppConfig,
    run_index: int,
    reports_dir: Path,
    run_stamp: str | None,
    seed: int,
    *,
    workers_requested: int,
) -> BenchmarkRow:
    if approach == "ray":

        def run_fn(g: Graph, *, seed: int):
            return run_lpa_ray(
                g,
                max_iter=cfg.lpa_max_iter,
                chunk_divisor=cfg.lpa_chunk_divisor,
                num_cpus=effective_ray_cpus(cfg),
                ray_head_address=cfg.ray_head_address,
                seed=seed,
            )

    elif approach == "dask":
        from dask_impl.lpa_dask import run_lpa_dask

        def run_fn(g: Graph, *, seed: int):
            return run_lpa_dask(
                g,
                max_iter=cfg.lpa_max_iter,
                chunk_divisor=cfg.lpa_chunk_divisor,
                n_workers=effective_dask_workers(cfg),
                cfg=cfg,
                seed=seed,
            )

    else:
        raise ValueError(f"Unknown approach: {approach}")

    with track_memory_peaks() as mem:
        res = run_fn(graph, seed=seed)
    if approach == "ray":
        _shutdown_ray()
    run_cfg = replace(cfg, seed=seed)
    return _row_from_result(
        approach,
        fraction_pct,
        run_cfg,
        mem,
        res,
        run_index,
        reports_dir,
        run_stamp,
        graph_load_time_s,
        workers_requested=workers_requested,
    )


def _fail_row(
    *,
    approach: str,
    fraction_pct: float,
    run_index: int,
    graph_load_time_s: float,
    cfg: AppConfig,
    run_seed: int,
    exc: Exception,
    workers_requested: int,
) -> BenchmarkRow:
    return BenchmarkRow(
        approach=approach,
        fraction_pct=fraction_pct,
        run_index=run_index,
        node_count=0,
        graph_load_time_s=graph_load_time_s,
        init_time_s=0.0,
        algorithm_time_s=0.0,
        total_time_s=0.0,
        peak_memory_mb=0.0,
        peak_driver_rss_mb=0.0,
        peak_process_tree_rss_mb=0.0,
        throughput_nodes_per_s=0.0,
        num_communities=0,
        num_levels=0,
        max_iter=float(cfg.lpa_max_iter),
        seed=run_seed,
        converged=False,
        status="failed",
        error_message=str(exc),
        level_times_json="[]",
        partition_summary="",
        communities_json="",
        vm_peaks_json="{}",
        peak_cluster_rss_mb=0.0,
        workers_requested=workers_requested,
        workers_actual=0,
    )


def run_benchmark_campaign(
    graph_path: Path,
    output_csv: Path,
    runs: int = 3,
    fractions: list[float] | None = None,
    workers_list: list[int] | None = None,
    cfg: AppConfig | None = None,
    log_path: Path | None = None,
    run_stamp: str | None = None,
    approaches: list[str] | None = None,
    append: bool = False,
) -> Path:
    cfg = cfg or load_config()
    fractions = fractions or [100.0]
    effective_workers = workers_list or [cfg.lpa_chunk_divisor]
    selected = approaches or list(DEFAULT_APPROACHES)
    rows: list[BenchmarkRow] = []
    reports_dir = output_csv.parent

    log_file = None
    if log_path is not None:
        log_file = open_benchmark_log(log_path, append=append)
        if not append:
            write_log_header(
                log_file,
                stamp=run_stamp or "unknown",
                cfg=cfg,
                graph_path=graph_path,
            )

    runners = {"ray", "dask"}
    seeds = resolve_benchmark_seeds(cfg, runs)

    try:
        for frac in fractions:
            # Graph sampling uses cfg.seed once per fraction; run-to-run variance
            # in the benchmark comes from LPA seeds (resolve_benchmark_seeds), not
            # from resampling the subgraph on each run.
            if is_graph_artifact(graph_path):
                print(f"[benchmark] loading fixture {graph_path} ...", flush=True)
                from preprocessing.graph_artifact import load_graph_artifact

                loaded = load_graph_artifact(graph_path)
            else:
                loaded = load_fraction_for_benchmark(
                    graph_path,
                    fraction_pct=frac,
                    seed=cfg.seed,
                    dataset_slug=cfg.dataset_slug,
                    directed=cfg.graph_directed,
                )
            frac_label = loaded.fraction_pct if is_graph_artifact(graph_path) else frac
            print(
                f"  {loaded.node_count:,} nodes, {loaded.edge_count:,} directed edges "
                f"in {loaded.load_time_s:.1f}s",
                flush=True,
            )
            est = estimate_for_fraction(
                frac_label, loaded.node_count, loaded.edge_count
            )
            print(format_estimate_report(est), flush=True)

            for w in effective_workers:
                cfg_w = replace(cfg, lpa_chunk_divisor=w)
                for approach in selected:
                    if approach not in runners:
                        raise ValueError(f"Unknown approach: {approach}")
                    for run_idx in range(1, runs + 1):
                        run_seed = seeds[run_idx - 1]
                        label = (
                            f"fraction={frac_label}% workers={w} "
                            f"run={run_idx} seed={run_seed}"
                        )
                        if log_file is not None:
                            write_log_section(log_file, approach, label)
                        try:
                            if log_file is not None:
                                with approach_log_capture(log_file, approach):
                                    row = _run_approach(
                                        approach,
                                        loaded.graph,
                                        frac_label,
                                        loaded.load_time_s,
                                        cfg_w,
                                        run_idx,
                                        reports_dir,
                                        run_stamp,
                                        run_seed,
                                        workers_requested=w,
                                    )
                            else:
                                row = _run_approach(
                                    approach,
                                    loaded.graph,
                                    frac_label,
                                    loaded.load_time_s,
                                    cfg_w,
                                    run_idx,
                                    reports_dir,
                                    run_stamp,
                                    run_seed,
                                    workers_requested=w,
                                )
                            rows.append(row)
                            if log_file is not None:
                                write_log_summary(
                                    log_file, approach, label, asdict(row)
                                )
                        except Exception as exc:  # noqa: BLE001
                            _shutdown_ray()
                            fail_row = _fail_row(
                                approach=approach,
                                fraction_pct=frac_label,
                                run_index=run_idx,
                                graph_load_time_s=loaded.load_time_s,
                                cfg=cfg_w,
                                run_seed=run_seed,
                                exc=exc,
                                workers_requested=w,
                            )
                            rows.append(fail_row)
                            if log_file is not None:
                                write_log_summary(
                                    log_file, approach, label, asdict(fail_row)
                                )
    finally:
        _shutdown_ray()
        if log_file is not None:
            close_benchmark_log(log_file)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not (append and output_csv.is_file())
    mode = "a" if append and output_csv.is_file() else "w"
    with output_csv.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    return output_csv
