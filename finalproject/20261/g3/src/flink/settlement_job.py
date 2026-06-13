"""Job de liquidação (settlement) — núcleo do exactly-once.

Fluxo:
  tx.raw (JSON)
    -> watermark (event-time, bounded out-of-orderness) — guia a ordenação por
       conta e as métricas
    -> flat_map: expande cada transação em ops de ledger keyed por conta
    -> key_by(account)
    -> Ledger (KeyedProcessFunction): saldo por conta (ValueState/RocksDB);
       cada op é BUFFERIZADA e aplicada em ordem de event-time (timer de event-time
       guiado por watermark). DEBIT aceito por SALDO SUFICIENTE => SETTLED, senão
       REJECTED; CREDIT sempre soma. Um timer de processing-time drena a cauda
       quando a entrada cessa (watermark para de avançar numa fonte Kafka ilimitada).
    -> tx.outcomes (Kafka, sink EXACTLY_ONCE | AT_LEAST_ONCE)

A aceitação por saldo depende da ordem; por isso o stream aplica as ops de cada
conta na MESMA ordem determinística do baseline batch — ``(event_time,
transaction_id, kind)``. Sob exactly-once o saldo final por conta é idêntico ao
baseline -> reconciliação exata ao centavo.

Estado por conta: saldo (ValueState) + buffer de ops pendentes (ListState) +
contador de aplicação (ValueState) em RocksDB + checkpoints incrementais no MinIO.
Cada outcome carrega o saldo após a op e um ``apply_seq`` (sequência monotônica de
aplicação por conta); o consumer mantém o de maior ``apply_seq`` — o último op de
fato aplicado ao estado —, o que reporta o saldo final mesmo se um evento atrasado
for aplicado fora da ordem de event-time.
"""

from __future__ import annotations

import json
import sys
import time as _t

sys.path.insert(0, "/app/src")

from pyflink.common import Types                                  # noqa: E402
from pyflink.datastream import TimeDomain                         # noqa: E402
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext  # noqa: E402
from pyflink.datastream.state import (                            # noqa: E402
    ValueStateDescriptor, ListStateDescriptor,
)

from common.schema import Transaction                            # noqa: E402
from common.ledger import (                                      # noqa: E402
    expand_to_ops, OPENING_BALANCE_CENTS, DEBIT,
)
from flink.common.flink_common import (                          # noqa: E402
    build_env, watermark_strategy, kafka_source, kafka_sink, env_int,
)

RAW_TOPIC = "tx.raw"
OUT_TOPIC = "tx.outcomes"

# Ociosidade (ms) após a qual a cauda bufferizada é drenada por processing-time.
# A fonte Kafka é ilimitada: quando o producer termina, o watermark para de
# avançar e as últimas ops nunca disparariam por event-time. Esse timer garante
# que o saldo final feche mesmo assim. Default 30s.
DRAIN_IDLE_MS = env_int("SETTLE_DRAIN_MS", 30_000)

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
    """Saldo keyed por conta; bufferiza ops e aplica em ordem de event-time.

    DEBIT aceito por SALDO SUFICIENTE. A ordenação determinística por
    ``(event_time, transaction_id, kind)`` reproduz, por conta, a mesma ordem do
    baseline batch — preservando a reconciliação exata.
    """

    def open(self, ctx: RuntimeContext):
        # Tipo construído localmente (não como global) para não ser capturado pelo
        # cloudpickle ao serializar esta função do módulo __main__.
        op_type = Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
            Types.STRING(), Types.STRING(), Types.INT(), Types.LONG(),
        ])
        self.balance = ctx.get_state(
            ValueStateDescriptor("balance", Types.LONG()))
        self.buffer = ctx.get_list_state(
            ListStateDescriptor("buffer", op_type))
        self.drain_timer = ctx.get_state(
            ValueStateDescriptor("drain_timer", Types.LONG()))
        # Contador monotônico de aplicação por conta. O outcome de maior apply_seq
        # é o ÚLTIMO efetivamente aplicado ao estado -> carrega o saldo final. O
        # consumer reconstrói o saldo final mantendo o de maior apply_seq, o que é
        # robusto mesmo se eventos atrasados forem aplicados fora da ordem de
        # event-time (a chave event_time não basta nesse caso).
        self.applied = ctx.get_state(
            ValueStateDescriptor("applied", Types.LONG()))

    def process_element(self, op, ctx):
        self.buffer.add(op)
        etime = op[2]
        ctx.timer_service().register_event_time_timer(etime)

        prev = self.drain_timer.value()
        if prev is not None:
            ctx.timer_service().delete_processing_time_timer(prev)
        drain_at = ctx.timer_service().current_processing_time() + DRAIN_IDLE_MS
        ctx.timer_service().register_processing_time_timer(drain_at)
        self.drain_timer.update(drain_at)

    def on_timer(self, timestamp, ctx):
        if ctx.time_domain() == TimeDomain.EVENT_TIME:
            cutoff = timestamp           # libera ops com event_time <= watermark
        else:
            cutoff = None                # drain da cauda: aplica tudo
            self.drain_timer.clear()

        due, pending = [], []
        for op in self.buffer.get():
            if cutoff is None or op[2] <= cutoff:
                due.append(op)
            else:
                pending.append(op)
        if not due:
            return

        due.sort(key=lambda o: (o[2], o[1], o[4]))  # (event_time, tid, kind)

        bal = self.balance.value()
        if bal is None:
            bal = OPENING_BALANCE_CENTS
        seq = self.applied.value() or 0

        for account, tid, etime, delta, kind, ttype, is_fraud, ingest in due:
            if kind == DEBIT:
                amount = -delta
                if amount <= bal:
                    bal -= amount
                    status = "SETTLED"
                else:
                    status = "REJECTED"
            else:
                bal += delta
                status = "SETTLED"
            seq += 1
            yield json.dumps({
                "transaction_id": tid,
                "account": account,
                "op_kind": kind,
                "status": status,
                "balance_cents": bal,
                "apply_seq": seq,
                "event_time": etime,
                "tx_type": ttype,
                "is_fraud": is_fraud,
                "ingest_time_ms": ingest,
                "settle_time_ms": int(_t.time() * 1000),
            })

        self.balance.update(bal)
        self.applied.update(seq)
        self.buffer.update(pending)


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
