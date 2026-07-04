"""Unweighted directed graph stored as out-CSR."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

import numpy as np

_NODE_DTYPE = np.int32
_INDEX_DTYPE = np.int32
_INDPTR_DTYPE = np.int32


def _unique_union_sorted(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    if a.size == 0:
        return np.unique(b)
    if b.size == 0:
        return np.unique(a)
    return np.union1d(np.unique(a), np.unique(b))


def symmetrize_coo(
    src: np.ndarray,
    dst: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Duplicate each arc in reverse (undirected SNAP line → out-CSR for LPA)."""
    if src.size == 0:
        return src, dst
    return (
        np.concatenate([src, dst]),
        np.concatenate([dst, src]),
    )


def _dedupe_directed_edges(
    src: np.ndarray,
    dst: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Drop self-loops and duplicate (src, dst) pairs."""
    src = np.ascontiguousarray(src, dtype=_NODE_DTYPE)
    dst = np.ascontiguousarray(dst, dtype=_NODE_DTYPE)

    mask = src != dst
    if not mask.all():
        src, dst = src[mask], dst[mask]
    if src.size == 0:
        return src, dst

    order = np.lexsort((dst, src))
    src = src[order]
    dst = dst[order]

    change = np.ones(len(src), dtype=bool)
    change[1:] = (src[1:] != src[:-1]) | (dst[1:] != dst[:-1])
    starts = np.flatnonzero(change)
    return src[starts], dst[starts]


def _csr_from_edge_rows(
    rows: np.ndarray,
    cols: np.ndarray,
    n: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Build CSR where ``rows[i] -> cols[i]`` (both internal indices)."""
    nnz = len(rows)
    if nnz == 0:
        return np.zeros(n + 1, dtype=_INDPTR_DTYPE), np.empty(0, dtype=_INDEX_DTYPE)

    order = np.argsort(rows, kind="mergesort")
    rows = rows[order]
    cols = cols[order]

    counts = np.bincount(rows, minlength=n)
    indptr = np.zeros(n + 1, dtype=_INDPTR_DTYPE)
    indptr[1:] = np.cumsum(counts, dtype=_INDPTR_DTYPE)

    _, group_starts = np.unique(rows, return_index=True)
    group_counts = np.diff(np.append(group_starts, nnz))
    local_rank = np.arange(nnz, dtype=_INDEX_DTYPE) - np.repeat(
        group_starts, group_counts
    )
    dest = indptr[rows] + local_rank

    neighbors = np.empty(nnz, dtype=_INDEX_DTYPE)
    neighbors[dest] = cols
    return indptr, neighbors


def _out_csr_from_coo(
    src: np.ndarray,
    dst: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    """
    Build out-CSR for directed edges ``src -> dst``.

    ``neighbors`` lists out-neighbor internal indices for each node.
    """
    node_ids = _unique_union_sorted(src, dst)
    row_src = np.searchsorted(node_ids, src).astype(_INDEX_DTYPE, copy=False)
    row_dst = np.searchsorted(node_ids, dst).astype(_INDEX_DTYPE, copy=False)
    n = len(node_ids)
    m = len(src)

    indptr, neighbors = _csr_from_edge_rows(row_src, row_dst, n)
    return node_ids, indptr, neighbors, float(m)


@dataclass
class Graph:
    """
    Directed simple graph in out-CSR form.

    ``neighbor_indices(u)`` returns out-neighbors of ``u`` (LPA votes on
    stored arcs; undirected SNAP graphs are symmetrized at load time).
    """

    node_ids: np.ndarray
    indptr: np.ndarray
    neighbors: np.ndarray
    _m: float

    @property
    def num_nodes(self) -> int:
        return len(self.node_ids)

    @property
    def m(self) -> float:
        return self._m

    @property
    def out_degrees(self) -> np.ndarray:
        return np.diff(self.indptr).astype(np.float64)

    def __contains__(self, node_id: int) -> bool:
        idx = int(np.searchsorted(self.node_ids, node_id))
        return idx < len(self.node_ids) and int(self.node_ids[idx]) == node_id

    def node_index(self, node_id: int) -> int:
        idx = int(np.searchsorted(self.node_ids, node_id))
        if idx >= len(self.node_ids) or int(self.node_ids[idx]) != node_id:
            raise KeyError(node_id)
        return idx

    def init_labels(self) -> np.ndarray:
        return self.node_ids.astype(np.int64, copy=True)

    def labels_from_map(self, mapping: dict[int, int]) -> np.ndarray:
        labels = self.init_labels()
        for node_id, community in mapping.items():
            if node_id in self:
                labels[self.node_index(node_id)] = community
        return labels

    def labels_to_dict(self, labels: np.ndarray) -> dict[int, int]:
        return {
            int(node): int(label)
            for node, label in zip(self.node_ids, labels, strict=True)
        }

    def out_degree_of(self, node_id: int) -> float:
        if node_id not in self:
            return 0.0
        idx = self.node_index(node_id)
        return float(self.indptr[idx + 1] - self.indptr[idx])

    def degree_of(self, node_id: int) -> float:
        return self.out_degree_of(node_id)

    def neighbor_indices_at(self, idx: int) -> np.ndarray:
        """Out-neighbor internal indices by CSR row (``idx``), O(1) lookup."""
        start = int(self.indptr[idx])
        end = int(self.indptr[idx + 1])
        return self.neighbors[start:end]

    def neighbor_indices(self, node_id: int) -> np.ndarray:
        """Out-neighbor internal indices by external node id."""
        return self.neighbor_indices_at(self.node_index(node_id))

    @classmethod
    def from_nodes(cls, node_ids: list[int] | np.ndarray) -> Graph:
        ids = np.unique(np.asarray(node_ids, dtype=_NODE_DTYPE))
        ids.sort()
        n = len(ids)
        return cls(
            node_ids=ids,
            indptr=np.zeros(n + 1, dtype=_INDPTR_DTYPE),
            neighbors=np.empty(0, dtype=_INDEX_DTYPE),
            _m=0.0,
        )

    @classmethod
    def from_undirected_edges(cls, edges: list[tuple[int, int]]) -> Graph:
        """Build a directed graph from undirected pairs (both directions added)."""
        if not edges:
            return cls.from_nodes([])
        expanded: list[tuple[int, int]] = []
        for u, v in edges:
            if u == v:
                continue
            expanded.append((u, v))
            expanded.append((v, u))
        return cls.from_edges(expanded)

    @classmethod
    def from_csr_arrays(
        cls,
        node_ids: np.ndarray,
        indptr: np.ndarray,
        neighbors: np.ndarray,
        m: float,
    ) -> Graph:
        return cls(
            node_ids=np.ascontiguousarray(node_ids, dtype=_NODE_DTYPE),
            indptr=np.ascontiguousarray(indptr, dtype=_INDPTR_DTYPE),
            neighbors=np.ascontiguousarray(neighbors, dtype=_INDEX_DTYPE),
            _m=float(m),
        )

    @classmethod
    def from_edges(cls, edges: list[tuple[int, int]]) -> Graph:
        if not edges:
            return cls.from_nodes([])
        src = np.fromiter((e[0] for e in edges), dtype=_NODE_DTYPE, count=len(edges))
        dst = np.fromiter((e[1] for e in edges), dtype=_NODE_DTYPE, count=len(edges))
        return cls.from_coo(src, dst)

    @classmethod
    def from_coo(
        cls,
        src: np.ndarray,
        dst: np.ndarray,
        *,
        already_deduped: bool = False,
    ) -> Graph:
        if already_deduped:
            s, d = src, dst
        else:
            s, d = _dedupe_directed_edges(src, dst)
        if s.size == 0:
            return cls.from_nodes([])

        node_ids, indptr, neighbors, total_m = _out_csr_from_coo(s, d)
        return cls(
            node_ids=node_ids,
            indptr=indptr,
            neighbors=neighbors,
            _m=total_m,
        )

    def iter_directed_edges(self) -> Iterator[tuple[int, int]]:
        """Yield stored directed arcs (u, v)."""
        for i in range(len(self.node_ids)):
            u = int(self.node_ids[i])
            start = int(self.indptr[i])
            end = int(self.indptr[i + 1])
            for j in self.neighbors[start:end]:
                yield u, int(self.node_ids[int(j)])

    def edges(self) -> list[tuple[int, int]]:
        return list(self.iter_directed_edges())
