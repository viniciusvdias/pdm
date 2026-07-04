"""Tests for Ray cluster vs local init."""

import logging
from contextlib import ExitStack
from unittest.mock import patch

from graph.graph import Graph
from ray_impl.lpa_ray import run_lpa_ray


def _enter_ray_patches(stack: ExitStack):
    stack.enter_context(patch("ray_impl.lpa_ray.ray.is_initialized", return_value=False))
    stack.enter_context(patch("ray_impl.lpa_ray.ray.put", side_effect=lambda x: x))
    stack.enter_context(patch("ray_impl.lpa_ray.ray.get", side_effect=lambda refs: refs))
    stack.enter_context(
        patch(
            "ray_impl.lpa_ray.lpa_chunk_remote.remote",
            side_effect=lambda chunk, graph, snap: (
                snap[chunk],
                {"host": "ray-worker", "peak_rss_mb": 128.0},
            ),
        )
    )


def test_run_lpa_ray_local_init(tiny_graph: Graph):
    with ExitStack() as stack:
        _enter_ray_patches(stack)
        mock_init = stack.enter_context(patch("ray_impl.lpa_ray.ray.init"))
        run_lpa_ray(tiny_graph, num_cpus=2, max_iter=1)
        mock_init.assert_called_once_with(
            num_cpus=2, ignore_reinit_error=True, logging_level=logging.WARNING
        )


def test_run_lpa_ray_cluster_init(tiny_graph: Graph):
    with ExitStack() as stack:
        _enter_ray_patches(stack)
        mock_init = stack.enter_context(patch("ray_impl.lpa_ray.ray.init"))
        stack.enter_context(
            patch("ray_impl.lpa_ray.ray.cluster_resources", return_value={"CPU": 3.0})
        )
        run_lpa_ray(tiny_graph, ray_head_address="10.0.0.5", max_iter=1)
        mock_init.assert_called_once_with(
            address="ray://10.0.0.5:10001",
            ignore_reinit_error=True,
            logging_level=logging.WARNING,
        )
