"""Dask-based Label Propagation implementation."""

from dask_impl.lpa_dask import run_lpa_dask
from lpa_core.lpa import LpaResult

__all__ = ["run_lpa_dask", "LpaResult"]
