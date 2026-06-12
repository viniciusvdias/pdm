"""Job de liquidação (settlement) — núcleo do exactly-once.

Fluxo:
  tx.raw (JSON)
    -> watermark (event-time, bounded out-of-orderness) — usado para event-time
       e métricas; a corretude do settlement NÃO depende de ordem (ver ledger.py)
    -> flat_map: expande cada transação em ops de ledger keyed por conta
    -> key_by(account)
    -> Ledger (KeyedProcessFunction): saldo por conta (ValueState/RocksDB);
       DEBIT aceito por LIMITE POR TRANSAÇÃO (independente de ordem) => SETTLED,
       senão REJECTED; CREDIT sempre soma. Aplica na CHEGADA (sem buffering).
    -> tx.outcomes (Kafka, sink EXACTLY_ONCE | AT_LEAST_ONCE)

Como a aceitação é por limite (não por saldo corrente) e créditos são comutativos,
o saldo final por conta é idêntico ao baseline batch sob exactly-once, qualquer que
seja a ordem de processamento — base do experimento de reconciliação.

Estado por conta: saldo (ValueState) em RocksDB + checkpoints incrementais no MinIO.
O outcome carrega o saldo após a op e a chave (event_time, tid, kind); o consumer
reconstrói o saldo final mantendo o de maior chave (robusto a reordenação).
"""

from __future__ import annotations

import json
import sys
import time as _t

sys.path.insert(0, "/app/src")

from pyflink.common import Types                                  # noqa: E402
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext  # noqa: E402
from pyflink.datastream.state import ValueStateDescriptor        # noqa: E402

from common.schema import Transaction                            # noqa: E402
from common.ledger import (                                      # noqa: E402
    expand_to_ops, OPENING_BALANCE_CENTS, TX_LIMIT_CENTS, DEBIT,
)
from flink.common.flink_common import (                          # noqa: E402
    build_env, watermark_strategy, kafka_source, kafka_sink,
)

RAW_TOPIC = "tx.raw"
OUT_TOPIC = "tx.outcomes"

# Op após o flat_map: (account, tid, event_time, delta, kind, type, is_fraud, ingest_ms)
OP_TYPE = Types.TUPLE([
    Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
    Types.STRING(), Types.STRING(), Types.INT(), Types.LONG(),
])


def expand_json(value: str):
    """JSON da transação -> tuplas-op keyed por conta."""
    try:
        d = json.loads(value)
    except Exception:
        return
    tx = Transaction(
        transaction_id=d["transaction_id"],
        step=int(d.get("step", 0)),
        event_time=int(d["event_time"]),
        type=d["type"],
        amount_cents=int(d["amount_cents"]),
        name_orig=d["name_orig"],
        old_balance_orig_cents=int(d.get("old_balance_orig_cents", 0)),
        name_dest=d.get("name_dest", ""),
        old_balance_dest_cents=int(d.get("old_balance_dest_cents", 0)),
        is_fraud=int(d.get("is_fraud", 0)),
        is_flagged_fraud=int(d.get("is_flagged_fraud", 0)),
    )
    ingest = int(d.get("ingest_time_ms", 0))
    for op in expand_to_ops(tx):
        yield (op.account, op.transaction_id, op.event_time, op.delta_cents,
               op.kind, op.type, op.is_fraud, ingest)


class Ledger(KeyedProcessFunction):
    """Saldo keyed por conta; aplica cada op na chegada (limite por transação)."""

    def open(self, ctx: RuntimeContext):
        self.balance = ctx.get_state(
            ValueStateDescriptor("balance", Types.LONG()))

    def process_element(self, op, ctx):
        account, tid, etime, delta, kind, ttype, is_fraud, ingest = op
        bal = self.balance.value()
        if bal is None:
            bal = OPENING_BALANCE_CENTS
        if kind == DEBIT:
            amount = -delta
            if amount <= TX_LIMIT_CENTS:
                bal -= amount
                status = "SETTLED"
            else:
                status = "REJECTED"
        else:
            bal += delta
            status = "SETTLED"
        self.balance.update(bal)
        yield json.dumps({
            "transaction_id": tid,
            "account": account,
            "op_kind": kind,
            "status": status,
            "balance_cents": bal,
            "event_time": etime,
            "tx_type": ttype,
            "is_fraud": is_fraud,
            "ingest_time_ms": ingest,
            "settle_time_ms": int(_t.time() * 1000),
        })


def main():
    env = build_env()
    source = kafka_source(RAW_TOPIC, "settlement")
    stream = env.from_source(source, watermark_strategy(), "tx.raw")

    ops = stream.flat_map(expand_json, output_type=OP_TYPE)
    outcomes = (ops
                .key_by(lambda o: o[0], key_type=Types.STRING())
                .process(Ledger(), output_type=Types.STRING()))

    outcomes.sink_to(kafka_sink(OUT_TOPIC, "settle-tx"))
    env.execute("pix-settlement")


if __name__ == "__main__":
    main()
