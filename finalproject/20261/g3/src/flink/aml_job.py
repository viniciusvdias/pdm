from __future__ import annotations

import hashlib
import json
import sys

sys.path.insert(0, "/app/src")

from pyflink.common import Types                                  # noqa: E402
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext  # noqa: E402
from pyflink.datastream.state import ListStateDescriptor         # noqa: E402

from flink.common.flink_common import (                          # noqa: E402
    build_env, watermark_strategy, kafka_source, kafka_sink, env_int,
)

RAW_TOPIC = "tx.raw"
ALERT_TOPIC = "aml.alerts"

#Evento normalizado p/ AML: (orig, dest, amount, event_time, tid, is_fraud, type)
EV_TYPE = Types.TUPLE([
    Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
    Types.STRING(), Types.INT(), Types.STRING(),
])


def parse_event(value: str):
    try:
        d = json.loads(value)
    except Exception:
        return
    yield (d.get("name_orig", ""), d.get("name_dest", ""),
           int(d.get("amount_cents", 0)), int(d["event_time"]),
           d.get("transaction_id", ""), int(d.get("is_fraud", 0)),
           d.get("type", ""))


def _alert(pattern: str, accounts: str, amount: int, etime: int, detail: str) -> str:
    aid = hashlib.sha1(f"{pattern}|{accounts}|{etime//60000}".encode()).hexdigest()[:16]
    return json.dumps({
        "alert_id": aid, "pattern": pattern, "accounts": accounts,
        "amount_cents": amount, "event_time": etime, "detail": detail,
    })


class VelocityStructuring(KeyedProcessFunction):
    """Velocidade e structuring por conta de origem (keyed)."""

    def open(self, ctx: RuntimeContext):
        # histórico de (event_time, amount, is_transfer_below)
        self.hist = ctx.get_list_state(ListStateDescriptor(
            "hist", Types.TUPLE([Types.LONG(), Types.LONG(), Types.INT()])))
        self.vel_n = env_int("AML_VELOCITY_N", 15)
        self.vel_w = env_int("AML_VELOCITY_WINDOW_S", 60) * 1000
        self.st_n = env_int("AML_STRUCT_N", 8)
        self.st_w = env_int("AML_STRUCT_WINDOW_S", 3600) * 1000
        self.st_below = env_int("AML_STRUCT_BELOW_CENTS", 1_000_000)

    def process_element(self, ev, ctx):
        orig, dest, amount, etime, tid, is_fraud, ttype = ev
        below = 1 if (ttype == "TRANSFER" and amount < self.st_below) else 0
        items = list(self.hist.get() or [])
        items.append((etime, amount, below))
        # poda pela maior janela
        horizon = etime - max(self.vel_w, self.st_w)
        items = [it for it in items if it[0] >= horizon]
        self.hist.update(items)

        # velocidade
        vel = [it for it in items if it[0] >= etime - self.vel_w]
        if len(vel) >= self.vel_n:
            yield _alert("VELOCITY", orig, sum(i[1] for i in vel), etime,
                         f"{len(vel)} tx em {self.vel_w//1000}s")
        # structuring
        st = [it for it in items if it[0] >= etime - self.st_w and it[2] == 1]
        if len(st) >= self.st_n:
            yield _alert("STRUCTURING", orig, sum(i[1] for i in st), etime,
                         f"{len(st)} transfers <{self.st_below}c em {self.st_w//1000}s")


class CycleDetector(KeyedProcessFunction):
    """Detecta ciclos A->B->C->A em arestas TRANSFER (paralelismo 1)."""

    def open(self, ctx: RuntimeContext):
        # arestas recentes: (src, dst, amount, event_time)
        self.edges = ctx.get_list_state(ListStateDescriptor(
            "edges", Types.TUPLE([Types.STRING(), Types.STRING(),
                                  Types.LONG(), Types.LONG()])))
        self.window = env_int("AML_CYCLE_WINDOW_S", 600) * 1000
        self.min_cents = env_int("AML_CYCLE_MIN_CENTS", 100_000_000)
        self.max_edges = 20000  # limite de segurança

    def process_element(self, ev, ctx):
        orig, dest, amount, etime, tid, is_fraud, ttype = ev
        if ttype != "TRANSFER" or not orig or not dest:
            return
        items = list(self.edges.get() or [])
        items.append((orig, dest, amount, etime))
        # poda por janela e por tamanho
        horizon = etime - self.window
        items = [e for e in items if e[3] >= horizon]
        if len(items) > self.max_edges:
            items = items[-self.max_edges:]
        self.edges.update(items)

        # adjacência das arestas recentes
        adj: dict[str, list] = {}
        for s, d, a, t in items:
            adj.setdefault(s, []).append((d, a))

        # DFS limitada a 3 saltos procurando voltar a `orig`
        target = orig
        for d1, a1 in adj.get(dest, []):
            if d1 == target and (amount + a1) >= self.min_cents:
                yield self._cycle(target, dest, d1, amount + a1, etime)
                continue
            for d2, a2 in adj.get(d1, []):
                if d2 == target and (amount + a1 + a2) >= self.min_cents:
                    yield self._cycle(target, dest, d1, amount + a1 + a2,
                                      etime, mid2=d1)

    def _cycle(self, a, b, c, total, etime, mid2=None):
        chain = f"{a}->{b}->{c}->{a}" if mid2 is None else f"{a}->{b}->{c}->{a}"
        return _alert("CYCLE", f"{a},{b},{c}", total, etime, chain)


def main():
    env = build_env()
    source = kafka_source(RAW_TOPIC, "aml")
    stream = env.from_source(source, watermark_strategy(), "tx.raw-aml")
    events = stream.flat_map(parse_event, output_type=EV_TYPE)

    velstruct = (events
                 .key_by(lambda e: e[0], key_type=Types.STRING())
                 .process(VelocityStructuring(), output_type=Types.STRING()))

    cycles = (events
              .key_by(lambda e: "g", key_type=Types.STRING())
              .process(CycleDetector(), output_type=Types.STRING())
              .set_parallelism(1))

    velstruct.union(cycles).sink_to(kafka_sink(ALERT_TOPIC, "aml-tx"))
    env.execute("pix-aml")


if __name__ == "__main__":
    main()
