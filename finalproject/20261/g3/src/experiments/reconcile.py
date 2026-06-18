from __future__ import annotations

import argparse
import os
import sys

import psycopg2


def load_baseline(path: str, burst_prefix: str) -> dict[str, int]:
    balances = {}
    with open(path, "r", encoding="utf-8") as f:
        next(f, None)  # header
        for line in f:
            line = line.strip()
            if not line:
                continue
            acct, bal = line.rsplit(",", 1)
            if acct.startswith(burst_prefix):
                continue
            balances[acct] = int(bal)
    return balances


def load_stream(burst_prefix: str) -> dict[str, int]:
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        user=os.environ.get("PGUSER", "pix"),
        password=os.environ.get("PGPASSWORD", "pix"),
        dbname=os.environ.get("PGDATABASE", "pix"),
    )
    cur = conn.cursor()
    cur.execute("SELECT account, balance_cents FROM account_balance;")
    balances = {a: int(b) for a, b in cur.fetchall()
                if not a.startswith(burst_prefix)}
    cur.close(); conn.close()
    return balances


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconciliação stream × batch")
    ap.add_argument("--baseline", required=True, help="CSV do baseline batch")
    ap.add_argument("--burst-prefix", default="BURST")
    ap.add_argument("--report", default="", help="opcional: CSV de divergências")
    args = ap.parse_args()

    base = load_baseline(args.baseline, args.burst_prefix)
    stream = load_stream(args.burst_prefix)

    accts = set(base) | set(stream)
    diffs = []
    total_abs = 0
    for a in accts:
        b = base.get(a)
        s = stream.get(a)
        if b is None or s is None or b != s:
            d = (s if s is not None else 0) - (b if b is not None else 0)
            total_abs += abs(d)
            diffs.append((a, b, s, d))

    print(f"Contas baseline={len(base)} stream={len(stream)} união={len(accts)}")
    print(f"Contas divergentes: {len(diffs)}")
    print(f"Soma |diferença| (centavos): {total_abs}")

    if args.report and diffs:
        with open(args.report, "w", encoding="utf-8") as out:
            out.write("account,baseline_cents,stream_cents,diff_cents\n")
            for a, b, s, d in sorted(diffs, key=lambda x: -abs(x[3])):
                out.write(f"{a},{b},{s},{d}\n")
        print(f"Divergências detalhadas -> {args.report}")

    if diffs:
        print("RESULTADO: DIVERGENTE (esperado em AT_LEAST_ONCE; falha em EXACTLY_ONCE)")
        for a, b, s, d in sorted(diffs, key=lambda x: -abs(x[3]))[:10]:
            print(f"  {a}: baseline={b} stream={s} diff={d}")
        return 2
    print("RESULTADO: RECONCILIADO — diferença ZERO em todas as contas. ✔")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
