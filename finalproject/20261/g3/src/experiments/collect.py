from __future__ import annotations

import argparse
import os
import statistics
import sys

import psycopg2


def pg():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        user=os.environ.get("PGUSER", "pix"),
        password=os.environ.get("PGPASSWORD", "pix"),
        dbname=os.environ.get("PGDATABASE", "pix"),
    )


def collect(elapsed_s: float) -> dict:
    conn = pg(); cur = conn.cursor()

    cur.execute("SELECT count(*), "
                "count(*) FILTER (WHERE status='SETTLED'), "
                "count(*) FILTER (WHERE status='REJECTED') FROM outcomes;")
    n, settled, rejected = cur.fetchone()

    cur.execute("SELECT settle_time_ms - ingest_time_ms FROM outcomes "
                "WHERE ingest_time_ms IS NOT NULL AND settle_time_ms IS NOT NULL "
                "AND settle_time_ms >= ingest_time_ms;")
    lat = [r[0] for r in cur.fetchall()]
    lat.sort()

    def pct(p):
        if not lat:
            return 0.0
        k = min(len(lat) - 1, int(round(p / 100 * (len(lat) - 1))))
        return float(lat[k])

    # AML precision/recall a nível de conta.
    cur.execute("SELECT DISTINCT account FROM outcomes WHERE is_fraud=1;")
    fraud_acc = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT accounts FROM aml_alerts;")
    flagged = set()
    for (accs,) in cur.fetchall():
        for a in (accs or "").split(","):
            a = a.strip()
            if a:
                flagged.add(a)
    tp = len(flagged & fraud_acc)
    precision = tp / len(flagged) if flagged else 0.0
    recall = tp / len(fraud_acc) if fraud_acc else 0.0

    cur.close(); conn.close()
    return {
        "outcomes": n or 0,
        "settled": settled or 0,
        "rejected": rejected or 0,
        "tps": (n or 0) / elapsed_s if elapsed_s > 0 else 0.0,
        "lat_p50": pct(50), "lat_p95": pct(95), "lat_p99": pct(99),
        "aml_precision": precision, "aml_recall": recall,
        "aml_flagged": len(flagged), "aml_fraud_accounts": len(fraud_acc),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Coleta métricas do Postgres")
    ap.add_argument("--elapsed", type=float, default=0.0,
                    help="tempo de parede da execução (s) p/ throughput")
    ap.add_argument("--out", default="", help="CSV de resultados (append)")
    ap.add_argument("--tag", default="", help="rótulo do experimento")
    ap.add_argument("--config", default="", help="config (ex.: 'parallelism=4')")
    args = ap.parse_args()

    m = collect(args.elapsed)
    for k, v in m.items():
        print(f"{k}={v}")

    if args.out:
        new = not os.path.exists(args.out)
        with open(args.out, "a", encoding="utf-8") as f:
            if new:
                f.write("tag,config," + ",".join(m.keys()) + "\n")
            f.write(f"{args.tag},{args.config}," +
                    ",".join(str(m[k]) for k in m) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
