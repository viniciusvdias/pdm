"""CLI entrypoint for distributed Label Propagation pipelines."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from benchmark.memory_estimate import estimate_for_fraction, format_estimate_report
from benchmark.metrics import track_memory_peaks
from benchmark.paths import (
    benchmark_run_stamp,
    metrics_csv_path,
    partitions_dir,
    read_run_stamp,
    resolve_report_paths,
    run_log_path,
    write_run_stamp,
)
from benchmark.partitions import write_partition
from benchmark.report import generate_report
from config import effective_dask_workers, effective_ray_cpus, load_config
from preprocessing.load import load_graph
from ray_impl.lpa_ray import run_lpa_ray


def _parse_fractions(value: str) -> list[float]:
    return [float(x.strip()) for x in value.split(",") if x.strip()]


def _parse_workers(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _load_graph_for_cli(
    input_path: Path,
    fraction_pct: float,
    seed: int,
    *,
    directed: bool = False,
):
    print(
        f"[load] SNAP {fraction_pct}% from {input_path} ...",
        flush=True,
    )
    with track_memory_peaks() as mem:
        loaded = load_graph(
            input_path,
            fraction_pct=fraction_pct,
            seed=seed,
            directed=directed,
        )
    est = estimate_for_fraction(
        fraction_pct, loaded.node_count, loaded.edge_count
    )
    print(
        f"  {loaded.node_count:,} nodes, {loaded.edge_count:,} directed edges "
        f"in {loaded.load_time_s:.1f}s "
        f"(driver RSS peak {mem.driver_rss_mb:.0f} MB)",
        flush=True,
    )
    print(format_estimate_report(est), flush=True)
    return loaded


def _emit_lpa_result(res, output_partition: str | None) -> None:
    print(json.dumps(res.summary_dict(), indent=2))
    if output_partition:
        arrays = res.partition_arrays()
        if arrays is None:
            print("ERROR: no partition labels to write", file=sys.stderr)
            return
        node_ids, labels_arr = arrays
        out = Path(output_partition)
        stem = out.stem
        parent = out.parent
        _summary_path, communities_path = write_partition(
            parent,
            stem,
            node_ids,
            labels_arr,
            {
                **res.summary_dict(),
                "labels_in_stdout": False,
            },
        )
        print(f"Wrote summary {parent / (stem + '.summary.json')}")
        print(f"Wrote communities {communities_path}")


def _cmd_lpa(args: argparse.Namespace, run_fn) -> int:
    cfg = load_config()
    input_path = Path(args.input or cfg.graph_raw_path)
    if not input_path.is_file():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1
    fraction = float(args.fraction if args.fraction is not None else 100.0)
    loaded = _load_graph_for_cli(
        input_path,
        fraction,
        args.seed or cfg.seed,
        directed=cfg.graph_directed,
    )
    res = run_fn(loaded.graph, args, cfg)
    _emit_lpa_result(res, args.output_partition)
    return 0


def cmd_lpa_ray(args: argparse.Namespace) -> int:
    return _cmd_lpa(
        args,
        lambda graph, a, c: run_lpa_ray(
            graph,
            max_iter=a.max_iter or c.lpa_max_iter,
            chunk_divisor=c.lpa_chunk_divisor,
            num_cpus=a.num_cpus if a.num_cpus is not None else effective_ray_cpus(c),
            ray_head_address=c.ray_head_address,
            seed=a.seed or c.seed,
        ),
    )


def cmd_lpa_dask(args: argparse.Namespace) -> int:
    from dask_impl.lpa_dask import run_lpa_dask

    return _cmd_lpa(
        args,
        lambda graph, a, c: run_lpa_dask(
            graph,
            max_iter=a.max_iter or c.lpa_max_iter,
            chunk_divisor=c.lpa_chunk_divisor,
            n_workers=a.n_workers if a.n_workers is not None else effective_dask_workers(c),
            cfg=c,
            seed=a.seed or c.seed,
        ),
    )


def cmd_benchmark(args: argparse.Namespace) -> int:
    from benchmark.runner import run_benchmark_campaign

    cfg = load_config()
    if args.ray_only and args.dask_only:
        print("ERROR: use only one of --ray-only or --dask-only", file=sys.stderr)
        return 1
    if args.ray_only:
        approaches = ["ray"]
    elif args.dask_only:
        approaches = ["dask"]
    else:
        approaches = None

    input_path = Path(args.input or cfg.graph_raw_path)
    if not input_path.is_file():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1

    reports_dir = Path(cfg.reports_dir)
    if args.run_stamp:
        stamp = args.run_stamp
    elif args.append:
        stamp = read_run_stamp(reports_dir) or benchmark_run_stamp()
    else:
        stamp = benchmark_run_stamp()
    out = (
        Path(args.output_csv)
        if args.output_csv
        else metrics_csv_path(reports_dir, stamp)
    )
    if not args.output_csv and not args.append:
        write_run_stamp(reports_dir, stamp)
    log_out = run_log_path(reports_dir, stamp)
    run_benchmark_campaign(
        input_path,
        out,
        runs=args.runs,
        fractions=_parse_fractions(args.fractions),
        workers_list=_parse_workers(args.workers) or None,
        cfg=cfg,
        log_path=log_out,
        run_stamp=stamp,
        approaches=approaches,
        append=args.append,
    )
    print(f"Wrote {out}")
    print(f"Wrote {log_out}")
    print(f"Partitions: {partitions_dir(reports_dir, stamp)}")
    print(f"Run stamp: {stamp}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    cfg = load_config()
    reports_dir = Path(cfg.reports_dir)
    input_csv = Path(args.input_csv) if args.input_csv else None
    output_md = Path(args.output_md) if args.output_md else None
    csv_path, md_path = resolve_report_paths(reports_dir, input_csv, output_md)
    md = generate_report(csv_path, md_path)
    print(f"Wrote {md}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Distributed Label Propagation on SNAP graphs"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_graph_args(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--input",
            type=str,
            default=None,
            help="SNAP edge-list path (default: config graph_raw_path)",
        )
        p.add_argument(
            "--fraction",
            type=float,
            default=None,
            help="Connected sample fraction of LCC (default: 100)",
        )
        p.add_argument("--seed", type=int, default=None)

    p_ray = sub.add_parser("lpa-ray", help="Run Ray Label Propagation")
    add_graph_args(p_ray)
    p_ray.add_argument("--max-iter", type=int, default=None)
    p_ray.add_argument("--num-cpus", type=int, default=None)
    p_ray.add_argument(
        "--output-partition",
        type=str,
        default=None,
        help="Write communities JSON + summary (stem path, no extension)",
    )
    p_ray.set_defaults(func=cmd_lpa_ray)

    p_dask = sub.add_parser("lpa-dask", help="Run Dask Label Propagation")
    add_graph_args(p_dask)
    p_dask.add_argument("--max-iter", type=int, default=None)
    p_dask.add_argument("--n-workers", type=int, default=None)
    p_dask.add_argument(
        "--output-partition",
        type=str,
        default=None,
        help="Write communities JSON + summary (stem path, no extension)",
    )
    p_dask.set_defaults(func=cmd_lpa_dask)

    p_bench = sub.add_parser("benchmark", help="Full benchmark campaign")
    add_graph_args(p_bench)
    p_bench.add_argument("--output-csv", type=str, default=None)
    p_bench.add_argument("--runs", type=int, default=3)
    p_bench.add_argument("--fractions", type=str, default="100")
    p_bench.add_argument(
        "--workers",
        type=str,
        default="",
        help="Comma-separated worker counts, e.g. '2,4,6' (default: os.cpu_count())",
    )
    p_bench.add_argument("--ray-only", action="store_true")
    p_bench.add_argument("--dask-only", action="store_true")
    p_bench.add_argument(
        "--run-stamp",
        type=str,
        default=None,
        help="Fixed run stamp (metrics/partitions/log filenames)",
    )
    p_bench.add_argument(
        "--append",
        action="store_true",
        help="Append rows to an existing metrics CSV / run log",
    )
    p_bench.set_defaults(func=cmd_benchmark)

    p_rep = sub.add_parser("report", help="Markdown report from CSV")
    p_rep.add_argument("--input-csv", type=str, default=None)
    p_rep.add_argument("--output-md", type=str, default=None)
    p_rep.set_defaults(func=cmd_report)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
