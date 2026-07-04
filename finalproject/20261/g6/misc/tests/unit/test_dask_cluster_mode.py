"""Tests for Dask cluster vs LocalCluster init."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

from config import AppConfig
from dask_impl.lpa_dask import run_lpa_dask
from graph.graph import Graph


def _cfg(**kwargs) -> AppConfig:
    defaults = dict(
        graph_raw_path=Path("data/raw/soc-orkut-relationships.txt"),
        dataset_slug="orkut",
        reports_dir=Path("reports"),
        seed=42,
        lpa_max_iter=50,
        lpa_chunk_divisor=12,
        graph_directed=False,
        ray_num_cpus=None,
        dask_n_workers=3,
        ray_head_address=None,
        dask_scheduler_address=None,
    )
    defaults.update(kwargs)
    return AppConfig(**defaults)


def test_run_lpa_dask_local_cluster(tiny_graph: Graph):
    mock_client = MagicMock()
    mock_cluster = MagicMock()
    mock_client.scatter.return_value = "ref"

    def _gather_results(futures):
        return [
            (
                np.array([i], dtype=np.int64),
                {"host": f"vm{i+2}", "peak_rss_mb": 100.0 + i},
            )
            for i in range(len(futures))
        ]

    mock_client.gather.side_effect = _gather_results
    mock_client.submit.return_value = MagicMock()
    with patch(
        "dask_impl.lpa_dask.LocalCluster", return_value=mock_cluster
    ) as mock_lc, patch(
        "dask_impl.lpa_dask.Client", return_value=mock_client
    ), patch(
        "dask_impl.lpa_dask.Client", return_value=mock_client
    ):
        run_lpa_dask(tiny_graph, max_iter=1, cfg=_cfg())
        mock_lc.assert_called_once()
        assert mock_lc.call_args.kwargs["n_workers"] == 3
        mock_client.close.assert_called_once()
        mock_cluster.close.assert_called_once()


def test_run_lpa_dask_remote_scheduler(tiny_graph: Graph):
    mock_client = MagicMock()
    mock_client.scatter.return_value = "ref"

    def _gather_results(futures):
        return [
            (
                np.array([i], dtype=np.int64),
                {"host": f"vm{i+2}", "peak_rss_mb": 100.0 + i},
            )
            for i in range(len(futures))
        ]

    mock_client.gather.side_effect = _gather_results
    mock_client.submit.return_value = MagicMock()
    mock_client.scheduler_info.return_value = {"workers": {"w1": {}, "w2": {}}}
    with patch("dask_impl.lpa_dask.LocalCluster") as mock_lc, patch(
        "dask_impl.lpa_dask.Client", return_value=mock_client
    ) as mock_client_cls:
        run_lpa_dask(
            tiny_graph,
            max_iter=1,
            cfg=_cfg(dask_scheduler_address="10.0.0.5:8786"),
        )
        mock_lc.assert_not_called()
        mock_client.wait_for_workers.assert_called_once_with(n_workers=6, timeout=120)
        mock_client_cls.assert_called_once_with("tcp://10.0.0.5:8786")
