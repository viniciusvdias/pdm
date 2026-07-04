from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, "/app/src")
try:
    from common.schema import iter_transactions, open_text
    from common.ledger import expand_to_ops, apply_op, opening_for
except ModuleNotFoundError:  # execução local fora do container
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from common.schema import iter_transactions, open_text
    from common.ledger import expand_to_ops, apply_op, opening_for


def compute(input_path: str, burst_prefix: str = "BURST"):
    balances: dict[str, int] = {}
    settled = rejected = credited = 0

    ops = []
    with open_text(input_path) as f:
        for tx in iter_transactions(f):
            ops.extend(expand_to_ops(tx))

    ops.sort(key=lambda o: (o.event_time, o.transaction_id, o.kind))

    for op in ops:
        if op.account.startswith(burst_prefix):
            continue  # contas sintéticas de burst não entram na reconciliação
        bal = balances.get(op.account, opening_for(op.account))
        new_bal, status = apply_op(bal, op)
        balances[op.account] = new_bal
        if status == "REJECTED":
            rejected += 1
        elif op.kind == "CREDIT":
            credited += 1
        else:
            settled += 1
    return balances, settled, rejected, credited


def main() -> int:
    ap = argparse.ArgumentParser(description="Baseline batch de saldos")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True, help="CSV account,balance_cents")
    ap.add_argument("--burst-prefix", default="BURST")
    args = ap.parse_args()

    t0 = time.time()
    balances, settled, rejected, credited = compute(args.input, args.burst_prefix)
    dt = time.time() - t0

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as out:
        out.write("account,balance_cents\n")
        for acct in sorted(balances):
            out.write(f"{acct},{balances[acct]}\n")

    print(f"Baseline: {len(balances)} contas | débitos SETTLED={settled} "
          f"REJECTED={rejected} créditos={credited} | {dt:.2f}s -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
