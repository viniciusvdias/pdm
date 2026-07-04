"""Ray-based Label Propagation implementation."""

from lpa_core.lpa import LpaResult
from ray_impl.lpa_ray import run_lpa_ray

__all__ = ["run_lpa_ray", "LpaResult"]
