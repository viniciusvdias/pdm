"""Partition artifact tests."""

import json
from pathlib import Path

import numpy as np

from benchmark.partitions import build_communities_payload, write_partition


def test_write_partition_creates_summary_and_communities(tmp_path: Path):
    node_ids = np.array([0, 1, 2, 3], dtype=np.int64)
    labels = np.array([10, 10, 20, 20], dtype=np.int64)
    summary_path, communities_path = write_partition(
        tmp_path,
        "ray_pokec_1pct_run1",
        node_ids,
        labels,
        {"algorithm_time_s": 1.5, "approach": "ray"},
    )
    assert summary_path.is_file()
    assert communities_path.is_file()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["algorithm_time_s"] == 1.5
    assert summary["communities_json"] == communities_path.name

    communities = json.loads(communities_path.read_text(encoding="utf-8"))
    assert communities["num_communities"] == 2
    assert len(communities["clusters"]) == 2
    assert communities["clusters"][0]["node_ids"] == [0, 1]
    assert communities["clusters"][1]["node_ids"] == [2, 3]


def test_build_communities_payload_sorts_by_size():
    node_ids = np.array([1, 2, 3, 4, 5], dtype=np.int64)
    labels = np.array([100, 100, 200, 200, 200], dtype=np.int64)
    payload = build_communities_payload(node_ids, labels)
    assert payload["clusters"][0]["size"] == 3
    assert payload["clusters"][0]["node_ids"] == [3, 4, 5]
