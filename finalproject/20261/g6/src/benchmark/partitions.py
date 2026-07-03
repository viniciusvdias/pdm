"""Write LPA community partition artifacts (summary + communities JSON)."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

# Above this node count, communities JSON lists sizes only (no full node_ids).
COMMUNITIES_FULL_NODE_LIST_LIMIT = 50_000


def partition_stem(
    approach: str,
    dataset_slug: str,
    fraction_pct: float,
    run_index: int,
) -> str:
    frac_label = str(fraction_pct).replace(".", "p")
    return f"{approach}_{dataset_slug}_{frac_label}pct_run{run_index}"


def partition_summary_path(directory: Path, stem: str) -> Path:
    return directory / f"{stem}.summary.json"


def communities_json_path(directory: Path, stem: str) -> Path:
    return directory / f"{stem}.communities.json"


def build_communities_payload(
    node_ids: np.ndarray,
    labels: np.ndarray,
    *,
    approach: str | None = None,
    include_node_ids: bool = True,
) -> dict[str, Any]:
    """Group node ids by community (largest clusters first)."""
    by_community: dict[int, list[int]] = defaultdict(list)
    for node_id, community_id in zip(node_ids, labels, strict=True):
        by_community[int(community_id)].append(int(node_id))

    clusters = []
    for rank, (community_id, members) in enumerate(
        sorted(by_community.items(), key=lambda item: (-len(item[1]), item[0])),
        start=1,
    ):
        entry: dict[str, Any] = {
            "cluster_index": rank,
            "community_id": community_id,
            "size": len(members),
        }
        if include_node_ids:
            entry["node_ids"] = sorted(members)
        clusters.append(entry)

    payload: dict[str, Any] = {
        "num_communities": len(clusters),
        "include_node_ids": include_node_ids,
        "clusters": clusters,
    }
    if approach is not None:
        payload["approach"] = approach
    return payload


def build_communities_payload_from_dict(
    labels: dict[int, int],
    **kwargs: Any,
) -> dict[str, Any]:
    if not labels:
        return build_communities_payload(
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=np.int64),
            **kwargs,
        )
    nodes = sorted(labels.keys())
    node_ids = np.asarray(nodes, dtype=np.int64)
    communities = np.asarray([labels[n] for n in nodes], dtype=np.int64)
    include = len(nodes) <= COMMUNITIES_FULL_NODE_LIST_LIMIT
    return build_communities_payload(
        node_ids, communities, include_node_ids=include, **kwargs
    )


def write_communities(
    directory: Path,
    stem: str,
    node_ids: np.ndarray,
    labels: np.ndarray,
    *,
    approach: str | None = None,
) -> Path:
    """Write human-readable community membership JSON."""
    directory.mkdir(parents=True, exist_ok=True)
    path = communities_json_path(directory, stem)
    include_node_ids = len(node_ids) <= COMMUNITIES_FULL_NODE_LIST_LIMIT
    payload = build_communities_payload(
        node_ids,
        labels,
        approach=approach,
        include_node_ids=include_node_ids,
    )
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def write_partition(
    directory: Path,
    stem: str,
    node_ids: np.ndarray,
    labels: np.ndarray,
    summary: dict[str, Any],
) -> tuple[Path, Path]:
    """Write summary JSON and communities JSON for a run partition."""
    directory.mkdir(parents=True, exist_ok=True)
    summary_path = partition_summary_path(directory, stem)

    communities_path = write_communities(
        directory,
        stem,
        node_ids,
        labels,
        approach=summary.get("approach"),
    )

    payload = {
        **summary,
        "communities_json": communities_path.name,
        "node_count": int(len(node_ids)),
        "num_communities": len(np.unique(labels)) if len(labels) else 0,
    }
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return summary_path, communities_path
