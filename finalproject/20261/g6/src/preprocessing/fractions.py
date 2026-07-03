"""Shared fraction label helpers."""

from __future__ import annotations


def fraction_label(fraction_pct: float) -> str:
    """Encode fraction for filenames (e.g. 0.1 → ``0p1``, 100 → ``100``)."""
    if float(fraction_pct).is_integer():
        return str(int(fraction_pct))
    return str(fraction_pct).replace(".", "p")


def parse_fraction_label(label: str) -> float:
    """Decode fraction from a label segment."""
    return float(label.replace("p", "."))
