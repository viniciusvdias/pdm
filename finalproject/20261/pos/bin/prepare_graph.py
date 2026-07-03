#!/usr/bin/env python3
"""Convert a road-network GraphML into the Parquet contract used by the workloads.

Input: a GraphML road network (e.g. the ~1.1 GB RMSP graph derived from the
public Geofabrik OSM extract -- see README section 2). The GraphML is streamed
with ``iterparse`` so it never has to be fully loaded into memory.

Output (written to --output):
- vertices.parquet -> id, osm_id, lat, lon
- edges.parquet    -> src, dst, length_m, travel_time_s
- metadata.json    -> counts and byte sizes

Usage:
    python bin/prepare_graph.py --source road_network.graphml --output data/rmsp_graph
"""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def maybe_float(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def convert_graphml(source: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    vertices_path = output / "vertices.parquet"
    edges_path = output / "edges.parquet"

    key_names: dict[str, str] = {}
    node_to_idx: dict[str, int] = {}
    vertices: list[dict] = []
    edges: list[dict] = []

    current_kind: str | None = None
    current_attrib: dict[str, str] = {}
    current_data: dict[str, str] = {}

    context = ET.iterparse(source, events=("start", "end"))
    for event, elem in context:
        tag = local_name(elem.tag)

        if event == "start" and tag in {"node", "edge"}:
            current_kind = tag
            current_attrib = dict(elem.attrib)
            current_data = {}
            continue

        if event == "end" and tag == "data" and current_kind:
            key = elem.attrib.get("key", "")
            current_data[key_names.get(key, key)] = elem.text or ""
            elem.clear()
            continue

        if event != "end":
            continue

        if tag == "key":
            key_id = elem.attrib.get("id")
            attr_name = elem.attrib.get("attr.name")
            if key_id and attr_name:
                key_names[key_id] = attr_name

        elif tag == "node":
            osm_id = current_attrib["id"]
            idx = len(vertices)
            node_to_idx[osm_id] = idx
            vertices.append(
                {
                    "id": idx,
                    "osm_id": osm_id,
                    "lat": maybe_float(current_data.get("y")),
                    "lon": maybe_float(current_data.get("x")),
                }
            )
            current_kind = None
            current_attrib = {}
            current_data = {}

        elif tag == "edge":
            src = node_to_idx.get(current_attrib["source"])
            dst = node_to_idx.get(current_attrib["target"])
            if src is None or dst is None:
                elem.clear()
                continue
            length_m = maybe_float(current_data.get("length"))
            travel_time_s = maybe_float(current_data.get("travel_time"), length_m)
            if length_m <= 0:
                elem.clear()
                continue
            edges.append({"src": src, "dst": dst, "length_m": length_m, "travel_time_s": travel_time_s})
            current_kind = None
            current_attrib = {}
            current_data = {}

        elem.clear()

    vertices_df = pd.DataFrame(vertices)
    edges_df = pd.DataFrame(edges)
    vertices_df.to_parquet(vertices_path, index=False)
    edges_df.to_parquet(edges_path, index=False)

    metadata = {
        "source_graphml": str(source),
        "vertices": int(len(vertices_df)),
        "edges": int(len(edges_df)),
        "graphml_bytes": int(source.stat().st_size),
        "vertices_parquet_bytes": int(vertices_path.stat().st_size),
        "edges_parquet_bytes": int(edges_path.stat().st_size),
        "schema": {
            "vertices": ["id", "osm_id", "lat", "lon"],
            "edges": ["src", "dst", "length_m", "travel_time_s"],
        },
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="Path to the road_network.graphml file")
    parser.add_argument("--output", default="data/rmsp_graph")
    args = parser.parse_args()

    source = Path(args.source)
    if not source.exists():
        raise SystemExit(f"GraphML not found: {source}")

    metadata = convert_graphml(source, Path(args.output))
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
