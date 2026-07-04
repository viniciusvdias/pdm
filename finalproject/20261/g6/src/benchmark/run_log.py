"""Capture benchmark run logs (Dask/Ray workers, timestamps, config)."""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, TextIO

from config import AppConfig

LOG_LOGGER_NAMES = (
    "distributed",
    "distributed.scheduler",
    "distributed.worker",
    "distributed.nanny",
    "distributed.core",
    "distributed.http",
    "ray",
    "ray.worker",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class _LogFileHandler(logging.Handler):
    """Write Python logging records into the shared benchmark log."""

    def __init__(self, log_file: TextIO, tag: str) -> None:
        super().__init__()
        self._log_file = log_file
        self._tag = tag
        self.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        self._log_file.write(f"{_utc_now()} [{self._tag}] {msg}\n")
        self._log_file.flush()


class _TeeStream:
    """Write to original stream and log file with an approach tag."""

    def __init__(self, stream: TextIO, log_file: TextIO, tag: str) -> None:
        self._stream = stream
        self._log_file = log_file
        self._tag = tag
        self._buffer = ""

    def write(self, data: str) -> int:
        self._stream.write(data)
        self._buffer += data
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._log_file.write(f"{_utc_now()} [{self._tag}] {line}\n")
                self._log_file.flush()
        return len(data)

    def flush(self) -> None:
        self._stream.flush()
        if self._buffer.strip():
            self._log_file.write(f"{_utc_now()} [{self._tag}] {self._buffer}\n")
            self._log_file.flush()
            self._buffer = ""
        self._log_file.flush()

    def __getattr__(self, name: str):
        return getattr(self._stream, name)


def open_benchmark_log(log_path: Path, *, append: bool = False) -> TextIO:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append and log_path.is_file() else "w"
    return log_path.open(mode, encoding="utf-8")


@contextmanager
def approach_log_capture(log_file: TextIO, approach: str) -> Iterator[None]:
    """Capture stdout/stderr and framework logs for one approach (ray or dask)."""
    tag = approach.lower()
    handler = _LogFileHandler(log_file, tag)
    attached: list[tuple[str, logging.Handler]] = []

    for name in LOG_LOGGER_NAMES:
        logger = logging.getLogger(name)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        attached.append((name, handler))

    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = _TeeStream(old_stdout, log_file, f"{tag}-stdout")  # type: ignore[assignment]
    sys.stderr = _TeeStream(old_stderr, log_file, f"{tag}-stderr")  # type: ignore[assignment]

    try:
        yield
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        for name, h in attached:
            logging.getLogger(name).removeHandler(h)
        handler.flush()
        handler.close()
        log_file.flush()


def write_log_header(
    log_file: TextIO,
    *,
    stamp: str,
    cfg: AppConfig,
    graph_path: Path,
    graph_meta: dict | None = None,
) -> None:
    log_file.write(f"# benchmark run log — stamp {stamp}\n")
    log_file.write(f"# started {_utc_now()}\n")
    log_file.write(f"dataset_slug={cfg.dataset_slug}\n")
    log_file.write(f"graph={graph_path}\n")
    log_file.write(
        f"ray_num_cpus={cfg.ray_num_cpus} lpa_chunk_divisor={cfg.lpa_chunk_divisor} "
        f"graph_directed={cfg.graph_directed}\n"
    )
    log_file.write(
        f"dask_n_workers={cfg.dask_n_workers} "
        f"ray_head={cfg.ray_head_address} "
        f"dask_scheduler={cfg.dask_scheduler_address}\n"
    )
    log_file.write(f"seed={cfg.seed} lpa_max_iter={cfg.lpa_max_iter}\n")
    if graph_meta:
        log_file.write(
            f"nodes={graph_meta.get('node_count')} "
            f"edges={graph_meta.get('edge_count')}\n"
        )
    log_file.write("# ---\n")
    log_file.flush()


def write_log_section(log_file: TextIO, approach: str, label: str) -> None:
    log_file.write(f"\n# === {approach.upper()} — {label} @ {_utc_now()} ===\n")
    log_file.flush()


def write_log_summary(log_file: TextIO, approach: str, label: str, summary: dict) -> None:
    log_file.write(f"\n# --- {approach.upper()} summary {label} @ {_utc_now()} ---\n")
    for key, value in summary.items():
        log_file.write(f"{key}={value}\n")
    log_file.flush()


def close_benchmark_log(log_file: TextIO) -> None:
    log_file.write(f"\n# finished {_utc_now()}\n")
    log_file.flush()
    log_file.close()
