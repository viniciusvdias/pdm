from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, "/app/src")
from common.schema import iter_transactions, open_text  # noqa: E402
from producer.burst import BurstController   # noqa: E402

from confluent_kafka import Producer          # noqa: E402

TOPIC = "tx.raw"


def _delivery(err, msg):
    if err is not None:
        sys.stderr.write(f"[producer] falha de entrega: {err}\n")


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    input_csv = os.environ.get("INPUT_CSV", "/data/input.csv")
    rate = float(os.environ.get("RATE", "2000"))   # 0 => sem limite
    max_records = int(os.environ.get("MAX_RECORDS", "0"))  # 0 => todos

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
    })
    burst = BurstController()

    sent = 0
    t_start = time.time()
    # Orçamento de tokens para rate limiting (token bucket simples).
    bucket = 0.0
    last = time.time()

    print(f"[producer] iniciando: topic={TOPIC} rate={rate}/s "
          f"burst={burst.mode} input={input_csv}", flush=True)

    with open_text(input_csv) as f:
        for tx in iter_transactions(f):
            now = time.time()
            state = burst.update(now)
            eff_rate = rate * (state.multiplier if state.active else 1.0)

            # Rate limiting (se rate>0).
            if eff_rate > 0:
                bucket += (now - last) * eff_rate
                last = now
                if bucket < 1.0:
                    time.sleep(max(0.0, (1.0 - bucket) / eff_rate))
                    bucket = 1.0
                bucket -= 1.0

            rec = tx.to_dict()
            rec["ingest_time_ms"] = int(time.time() * 1000)
            rec["synthetic"] = False
            producer.produce(TOPIC, key=tx.transaction_id,
                             value=json.dumps(rec), on_delivery=_delivery)
            sent += 1

            # Injeção sintética de alto valor durante bursts.
            if state.active and burst.synthetic_enabled:
                for _ in range(int(state.multiplier)):
                    syn = burst.make_synthetic(tx.event_time)
                    syn["ingest_time_ms"] = int(time.time() * 1000)
                    producer.produce(TOPIC, key=syn["transaction_id"],
                                     value=json.dumps(syn), on_delivery=_delivery)
                    sent += 1

            if sent % 50000 == 0:
                producer.poll(0)
                el = time.time() - t_start
                print(f"[producer] {sent} enviados ({sent/el:.0f}/s) "
                      f"burst={'ON' if state.active else 'off'}", flush=True)

            if max_records and sent >= max_records:
                break

    producer.flush(30)
    el = time.time() - t_start
    print(f"[producer] CONCLUÍDO: {sent} transações em {el:.1f}s "
          f"({sent/el:.0f}/s média)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
