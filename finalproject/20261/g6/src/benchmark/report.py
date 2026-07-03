"""Generate Markdown comparison report from metrics CSV."""

from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

from benchmark.report_sections import LPA_PARALLELISM, REFERENCES

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@lru_cache(maxsize=8)
def _loc_count(glob_pattern: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in REPO_ROOT.glob(glob_pattern):
        if path.suffix == ".py":
            counts[str(path.relative_to(REPO_ROOT))] = sum(
                1
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
    return counts


def _has_workers_column(rows: list[dict[str, str]]) -> bool:
    return bool(rows) and "workers_requested" in rows[0]


def _mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


def _scalability_pivot(
    rows: list[dict[str, str]],
    *,
    approach: str,
    value_key: str,
    scale: float = 1.0,
) -> tuple[list[str], list[list[str]]]:
    filtered = [
        r
        for r in rows
        if r.get("approach") == approach and r.get("status") == "success"
    ]
    if not filtered:
        return [], []

    fractions = sorted({r["fraction_pct"] for r in filtered}, key=float)
    workers = sorted(
        {r.get("workers_requested", "?") for r in filtered},
        key=lambda x: float(x) if x not in ("", "?") else -1.0,
    )
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in filtered:
        grouped[(r["fraction_pct"], r.get("workers_requested", "?"))].append(
            float(r.get(value_key) or 0) * scale
        )

    header = ["Fração %", *workers]
    table: list[list[str]] = []
    for frac in fractions:
        row = [frac]
        for w in workers:
            vals = grouped.get((frac, w), [])
            row.append(f"{_mean(vals):.1f}" if vals else "—")
        table.append(row)
    return header, table


def _render_pivot_table(header: list[str], rows: list[list[str]]) -> list[str]:
    if not header:
        return []
    sep = "|" + "|".join(["---"] * len(header)) + "|"
    lines = [
        "| " + " | ".join(header) + " |",
        sep,
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def generate_report(input_csv: Path, output_md: Path) -> Path:
    rows: list[dict[str, str]] = []
    with input_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    success = [r for r in rows if r.get("status") == "success"]
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in success:
        groups[(r["approach"], r["fraction_pct"])].append(r)

    lines = [
        "# Relatório comparativo: Ray vs Dask",
        "",
        "## Resumo",
        "",
        "Comparação de Label Propagation distribuído no dataset soc-Orkut (carga direta do SNAP).",
        "",
        "## Desempenho (média ± desvio)",
        "",
        "Memória RSS = driver + workers locais (filhos do processo). "
        "`peak_cluster_rss_mb` = soma dos picos por hostname (VM). "
        "`peak_memory_mb` = heap Python (tracemalloc, só driver).",
        "",
        "| Abordagem | Fração % | Tempo total (s) | Tempo algo (s) | RSS total (MB) | RSS driver (MB) | Throughput (nós/s) |",
        "|-----------|----------|-----------------|----------------|----------------|-----------------|---------------------|",
    ]

    for (approach, frac), items in sorted(groups.items()):
        times = [float(x["algorithm_time_s"]) for x in items]
        totals = [float(x.get("total_time_s") or x["algorithm_time_s"]) for x in items]
        tree_mem = [
            float(x.get("peak_process_tree_rss_mb") or x.get("peak_memory_mb", 0))
            for x in items
        ]
        driver_mem = [
            float(x.get("peak_driver_rss_mb") or x.get("peak_memory_mb", 0))
            for x in items
        ]
        thr = [float(x["throughput_nodes_per_s"]) for x in items]
        lines.append(
            f"| {approach} | {frac} | "
            f"{statistics.mean(totals):.2f} ± {statistics.pstdev(totals) if len(totals) > 1 else 0:.2f} | "
            f"{statistics.mean(times):.2f} ± {statistics.pstdev(times) if len(times) > 1 else 0:.2f} | "
            f"{statistics.mean(tree_mem):.0f} ± {statistics.pstdev(tree_mem) if len(tree_mem) > 1 else 0:.0f} | "
            f"{statistics.mean(driver_mem):.0f} ± {statistics.pstdev(driver_mem) if len(driver_mem) > 1 else 0:.0f} | "
            f"{statistics.mean(thr):.0f} ± {statistics.pstdev(thr) if len(thr) > 1 else 0:.0f} |"
        )

    lines.extend(["", "## Qualidade", ""])
    for (approach, frac), items in sorted(groups.items()):
        comms = [int(x["num_communities"]) for x in items]
        lines.append(
            f"- **{approach} {frac}%**: comunidades≈{statistics.mean(comms):.0f}"
        )

    cluster_rows = [r for r in success if r.get("peak_cluster_rss_mb")]
    if cluster_rows:
        lines.extend(
            [
                "",
                "## Memória por VM (pico RSS)",
                "",
                "Soma `peak_cluster_rss_mb` ≈ pico total do cluster (driver + workers remotos).",
                "",
                "| Abordagem | Fração % | Run | Cluster RSS (MB) | Por VM (JSON) |",
                "|-----------|----------|-----|------------------|---------------|",
            ]
        )
        for r in sorted(
            cluster_rows,
            key=lambda x: (x["approach"], x["fraction_pct"], int(x["run_index"])),
        ):
            lines.append(
                f"| {r['approach']} | {r['fraction_pct']} | {r['run_index']} | "
                f"{float(r.get('peak_cluster_rss_mb') or 0):.0f} | "
                f"`{r.get('vm_peaks_json', '{}')}` |"
            )

    partition_rows = [r for r in success if r.get("communities_json")]
    if partition_rows:
        lines.extend(
            [
                "",
                "## Partições (clusters finais)",
                "",
                "Cada execução gera JSON resumo e `*.communities.json` com listas de nós por cluster.",
                "",
                "| Abordagem | Fração % | Run | Cluster RSS (MB) | Communities |",
                "|-----------|----------|-----|----------------|-------------|",
            ]
        )
        for r in sorted(
            partition_rows,
            key=lambda x: (x["approach"], x["fraction_pct"], int(x["run_index"])),
        ):
            lines.append(
                f"| {r['approach']} | {r['fraction_pct']} | {r['run_index']} | "
                f"{float(r.get('peak_cluster_rss_mb') or r.get('peak_process_tree_rss_mb') or 0):.0f} | "
                f"`{r.get('communities_json', '')}` |"
            )

    ray_loc = sum(_loc_count("src/ray_impl/**/*.py").values())
    dask_loc = sum(_loc_count("src/dask_impl/**/*.py").values())

    if _has_workers_column(rows):
        lines.extend(
            [
                "",
                "## Escalabilidade: Throughput por Fração × Workers",
                "",
                "Throughput médio (nós/s) por combinação. "
                "Valores de `algorithm_time_s` excluem BFS de amostragem "
                "(frações parciais usam artefacto pré-construído).",
                "",
            ]
        )
        for approach in ("ray", "dask"):
            header, table = _scalability_pivot(
                success, approach=approach, value_key="throughput_nodes_per_s"
            )
            if table:
                lines.append(f"### {approach.capitalize()}")
                lines.append("")
                lines.extend(_render_pivot_table(header, table))
                lines.append("")

        lines.extend(
            [
                "## Escalabilidade: Memória por Fração × Workers",
                "",
                "Pico RSS total (GB) = `peak_process_tree_rss_mb` / 1024.",
                "",
            ]
        )
        for approach in ("ray", "dask"):
            header, table = _scalability_pivot(
                success,
                approach=approach,
                value_key="peak_process_tree_rss_mb",
                scale=1 / 1024,
            )
            if table:
                lines.append(f"### {approach.capitalize()}")
                lines.append("")
                lines.extend(_render_pivot_table(header, table))
                lines.append("")

        lines.extend(
            [
                "## Nota: seeds e partições LPA",
                "",
                "O LPA distribuído síncrono (batch snapshot) produz **partições "
                "idênticas** independentemente do seed LPA: o desempate é "
                "determinístico e todos os chunks leem do mesmo snapshot por "
                "iteração. Runs com seeds distintas medem variabilidade "
                "**temporal** (tempo/memória), não variabilidade algorítmica.",
                "",
            ]
        )

    lines.extend(
        [
            "",
            "## Engenharia",
            "",
            f"- Linhas (aprox.) Ray: {ray_loc}",
            f"- Linhas (aprox.) Dask: {dask_loc}",
            "- Infra Ray: Python + pip",
            "- Infra Dask: Python + dask[distributed]",
            "",
            LPA_PARALLELISM,
            "",
            REFERENCES,
        ]
    )

    failed = [r for r in rows if r.get("status") != "success"]
    if failed:
        lines.extend(["", "## Execuções falhadas", ""])
        for r in failed:
            lines.append(
                f"- {r['approach']} {r['fraction_pct']}% run {r['run_index']}: {r.get('error_message', '')}"
            )

    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_md
