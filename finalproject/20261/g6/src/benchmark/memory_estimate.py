"""Extrapolate time/RAM from measured fractions to 100% Orkut (single host)."""

from __future__ import annotations

import math
from dataclasses import dataclass

# Measured integration runs (laptop, LocalCluster 3 workers, max_iter=50).
# Calibrated on soc-Pokec fractions; scaled to Orkut 100% by edge count.
_CALIBRATION = (
    {
        "fraction_pct": 0.1,
        "node_count": 1_632,
        "edge_count": 3_910,
        "ray_algo_s": 2.26,
        "dask_algo_s": 7.76,
        "ray_driver_rss_mb": 177.0,
        "dask_driver_rss_mb": 209.0,
        "ray_tree_rss_mb": 1_018.0,
        "dask_tree_rss_mb": 517.0,
    },
    {
        "fraction_pct": 10.0,
        "node_count": 163_280,
        "edge_count": 1_880_997,
        "ray_algo_s": 84.57,
        "dask_algo_s": 110.36,
        "ray_driver_rss_mb": 567.0,
        "dask_driver_rss_mb": 708.0,
        "ray_tree_rss_mb": 1_578.0,
        "dask_tree_rss_mb": 1_202.0,
    },
)

ORKUT_100_NODE_COUNT = 3_072_441
# Undirected SNAP lines symmetrized to out-CSR (both directions stored).
ORKUT_100_EDGE_COUNT = 234_370_166
ORKUT_100_EDGE_COUNT_LOW = 234_000_000
ORKUT_100_EDGE_COUNT_HIGH = 234_370_166

# Backward-compatible aliases for tests/tools that still import Pokec names.
POKEC_100_NODE_COUNT = ORKUT_100_NODE_COUNT
POKEC_100_EDGE_COUNT = ORKUT_100_EDGE_COUNT
POKEC_100_EDGE_COUNT_LOW = ORKUT_100_EDGE_COUNT_LOW
POKEC_100_EDGE_COUNT_HIGH = ORKUT_100_EDGE_COUNT_HIGH


@dataclass(frozen=True)
class GraphMemoryBytes:
    node_count: int
    edge_count: int
    csr_steady_bytes: int
    load_peak_bytes: int

    @property
    def csr_steady_mb(self) -> float:
        return self.csr_steady_bytes / (1024 * 1024)

    @property
    def load_peak_mb(self) -> float:
        return self.load_peak_bytes / (1024 * 1024)


@dataclass(frozen=True)
class TimeEstimate:
    ray_algo_s_low: float
    ray_algo_s_high: float
    dask_algo_s_low: float
    dask_algo_s_high: float

    @property
    def ray_minutes(self) -> tuple[float, float]:
        return self.ray_algo_s_low / 60, self.ray_algo_s_high / 60

    @property
    def dask_minutes(self) -> tuple[float, float]:
        return self.dask_algo_s_low / 60, self.dask_algo_s_high / 60


@dataclass(frozen=True)
class HostMemory:
    """Peak RSS (MB) on a single machine running local Ray/Dask workers."""

    load_peak_mb: float
    driver_ray_mb: float
    driver_dask_mb: float
    process_tree_ray_mb: float
    process_tree_dask_mb: float


@dataclass(frozen=True)
class FullEstimate:
    fraction_pct: float
    node_count: int
    edge_count: int
    graph: GraphMemoryBytes
    time: TimeEstimate
    cluster: HostMemory
    notes: tuple[str, ...]


def estimate_load_time_s(edge_count: int) -> tuple[float, float]:
    """Rough SNAP TXT→CSR load time (single-pass read; ~4.6 GB Orkut file)."""
    ref_edges = ORKUT_100_EDGE_COUNT
    ref_s = 420.0
    ratio = edge_count / ref_edges
    return ref_s * ratio * 0.7, ref_s * ratio * 1.4


def estimate_csr_bytes(node_count: int, edge_count: int) -> GraphMemoryBytes:
    """CSR steady size + conservative peak while building from edge stream.

    ``edge_count`` is the number of stored directed arcs in out-CSR (``graph.m``).
    """
    m = edge_count
    steady = (
        node_count * 4  # node_ids int32
        + (node_count + 1) * 4  # in-indptr
        + m * 4  # in-neighbors int32
    )
    coo = m * (4 + 4)
    transient = m * 4
    load_peak = steady + coo + transient
    return GraphMemoryBytes(
        node_count=node_count,
        edge_count=edge_count,
        csr_steady_bytes=steady,
        load_peak_bytes=load_peak,
    )


def extrapolate_time_to_100(
    *,
    edge_count_low: int = ORKUT_100_EDGE_COUNT_LOW,
    edge_count_high: int = ORKUT_100_EDGE_COUNT_HIGH,
) -> TimeEstimate:
    small, large = _CALIBRATION[0], _CALIBRATION[1]

    def _exp(key: str) -> float:
        ratio_y = large[key] / small[key]
        ratio_x = large["edge_count"] / small["edge_count"]
        if ratio_x <= 1 or ratio_y <= 1:
            return 1.0
        return math.log(ratio_y) / math.log(ratio_x)

    ray_exp = _exp("ray_algo_s")
    dask_exp = _exp("dask_algo_s")
    e10 = large["edge_count"]

    def _scale_low(algo_s: float, exp: float, e100: int) -> float:
        return algo_s * ((e100 / e10) ** exp)

    def _scale_high(algo_s: float, e100: int) -> float:
        return algo_s * (e100 / e10)

    return TimeEstimate(
        ray_algo_s_low=_scale_low(large["ray_algo_s"], ray_exp, edge_count_low),
        ray_algo_s_high=_scale_high(large["ray_algo_s"], edge_count_high),
        dask_algo_s_low=_scale_low(large["dask_algo_s"], dask_exp, edge_count_low),
        dask_algo_s_high=_scale_high(large["dask_algo_s"], edge_count_high),
    )


def estimate_host_memory(
    node_count: int,
    edge_count: int,
    *,
    single_host_tree_ray_mb: float | None = None,
    single_host_tree_dask_mb: float | None = None,
) -> HostMemory:
    """Scale integration RSS to target graph size (driver + local workers)."""
    graph = estimate_csr_bytes(node_count, edge_count)
    csr_mb = graph.csr_steady_mb
    load_mb = graph.load_peak_mb

    large = _CALIBRATION[1]
    tree_ray = single_host_tree_ray_mb or large["ray_tree_rss_mb"]
    tree_dask = single_host_tree_dask_mb or large["dask_tree_rss_mb"]

    ref_calib_csr = estimate_csr_bytes(
        large["node_count"], large["edge_count"]
    ).csr_steady_mb
    scale = max(1.0, csr_mb / ref_calib_csr) if ref_calib_csr else 1.0

    driver_ray = max(200.0, large["ray_driver_rss_mb"] - ref_calib_csr) + csr_mb * 1.1
    driver_dask = max(200.0, large["dask_driver_rss_mb"] - ref_calib_csr) + csr_mb * 1.1

    return HostMemory(
        load_peak_mb=load_mb * 0.9 + 300.0,
        driver_ray_mb=driver_ray,
        driver_dask_mb=driver_dask,
        process_tree_ray_mb=tree_ray * scale,
        process_tree_dask_mb=tree_dask * scale,
    )


def estimate_for_fraction(
    fraction_pct: float,
    node_count: int,
    edge_count: int,
) -> FullEstimate:
    """Build a full report for a target fraction (typically 100%)."""
    graph = estimate_csr_bytes(node_count, edge_count)
    if fraction_pct >= 100:
        time_est = extrapolate_time_to_100()
    else:
        ref = _CALIBRATION[1]
        ratio = edge_count / ref["edge_count"]
        time_est = TimeEstimate(
            ray_algo_s_low=ref["ray_algo_s"] * ratio * 0.6,
            ray_algo_s_high=ref["ray_algo_s"] * ratio,
            dask_algo_s_low=ref["dask_algo_s"] * ratio * 0.6,
            dask_algo_s_high=ref["dask_algo_s"] * ratio,
        )

    cluster = estimate_host_memory(node_count, edge_count)
    notes = (
        "Tempos extrapolados de integração 0,1% e 10% (lei de potência em arestas).",
        "Carga: TXT → numpy COO → out-CSR; undirected SNAP é simetrizado na carga.",
        "Memória: driver + workers locais na mesma VM (Ray/Dask em fases separadas).",
        "Workers/chunks: auto = CPU count (`LPA_WORKERS` para fixar).",
    )
    return FullEstimate(
        fraction_pct=fraction_pct,
        node_count=node_count,
        edge_count=edge_count,
        graph=graph,
        time=time_est,
        cluster=cluster,
        notes=notes,
    )


def format_estimate_report(est: FullEstimate) -> str:
    """Human-readable summary for logs / integration output."""
    g = est.graph
    t = est.time
    c = est.cluster
    ray_lo, ray_hi = t.ray_minutes
    dask_lo, dask_hi = t.dask_minutes
    load_lo, load_hi = estimate_load_time_s(est.edge_count)
    lines = [
        f"=== Estimativa {est.fraction_pct:g}% Orkut ===",
        f"  Nós: {est.node_count:,}  Arestas CSR: {est.edge_count:,}",
        f"  CSR estável: {g.csr_steady_mb:.0f} MB  |  Pico carga TXT→CSR: {g.load_peak_mb:.0f} MB",
        f"  Tempo carga: {load_lo:.0f}–{load_hi:.0f} s",
        f"  Tempo algo (100 iter): Ray {ray_lo:.0f}–{ray_hi:.0f} min | Dask {dask_lo:.0f}–{dask_hi:.0f} min",
        "  --- Memória (mesma VM) ---",
        f"  Pico carga TXT→CSR:        ~{c.load_peak_mb:.0f} MB",
        f"  Driver Ray (est.):         ~{c.driver_ray_mb:.0f} MB",
        f"  Driver Dask (est.):        ~{c.driver_dask_mb:.0f} MB",
        f"  Process tree Ray (est.):   ~{c.process_tree_ray_mb:.0f} MB",
        f"  Process tree Dask (est.):  ~{c.process_tree_dask_mb:.0f} MB",
    ]
    for note in est.notes:
        lines.append(f"  · {note}")
    return "\n".join(lines)
