from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict


def main() -> int:
    ap = argparse.ArgumentParser(description="Agrega runs -> média/desvio")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--group", default="config",
                    help="coluna de agrupamento (default: config)")
    args = ap.parse_args()

    rows = []
    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        for r in reader:
            rows.append(r)

    metrics = [c for c in fields if c not in ("tag", "config")]
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        g = r[args.group]
        for m in metrics:
            try:
                groups[g][m].append(float(r[m]))
            except (ValueError, KeyError):
                pass

    with open(args.output, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["config", "metric", "mean", "std", "runs"])
        for g in groups:
            for m in metrics:
                vals = groups[g][m]
                if not vals:
                    continue
                mean = statistics.mean(vals)
                std = statistics.stdev(vals) if len(vals) > 1 else 0.0
                w.writerow([g, m, f"{mean:.4f}", f"{std:.4f}", len(vals)])

    print(f"Agregado -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
