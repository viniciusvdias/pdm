"""Consumer: tx.outcomes + aml.alerts -> Postgres + métricas Prometheus.

- Mantém ``account_balance`` com o saldo final por conta (regra: maior
  ``order_key = event_time|tid|kind``), base da reconciliação.
- Grava a trilha ``outcomes`` e os ``aml_alerts``.
- Calcula latência end-to-end (settle_time_ms - ingest_time_ms) e expõe métricas
  Prometheus (``pix_outcomes_total``, ``pix_latency_ms``, ``pix_aml_alerts_total``)
  na porta ``METRICS_PORT`` para o Grafana.
"""

from __future__ import annotations

import json
import os
import sys
import time

from confluent_kafka import Consumer
import psycopg2
from psycopg2.extras import execute_batch
from prometheus_client import Counter, Histogram, start_http_server

OUT_TOPIC = "tx.outcomes"
ALERT_TOPIC = "aml.alerts"

OUTCOMES = Counter("pix_outcomes_total", "Outcomes de liquidação", ["status"])
ALERTS = Counter("pix_aml_alerts_total", "Alertas AML", ["pattern"])
LATENCY = Histogram(
    "pix_latency_ms", "Latência end-to-end (ms)",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000))


def connect_pg():
    while True:
        try:
            conn = psycopg2.connect(
                host=os.environ.get("PGHOST", "postgres"),
                user=os.environ.get("PGUSER", "pix"),
                password=os.environ.get("PGPASSWORD", "pix"),
                dbname=os.environ.get("PGDATABASE", "pix"),
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            sys.stderr.write(f"[consumer] aguardando Postgres: {e}\n")
            time.sleep(2)


def order_key(event_time: int, tid: str, kind: str) -> str:
    return f"{int(event_time):020d}|{tid}|{kind}"


UPSERT_BAL = """
INSERT INTO account_balance(account, balance_cents, order_key)
VALUES (%s, %s, %s)
ON CONFLICT (account) DO UPDATE
  SET balance_cents = EXCLUDED.balance_cents,
      order_key = EXCLUDED.order_key,
      updated_at = now()
  WHERE EXCLUDED.order_key > account_balance.order_key;
"""

UPSERT_OUT = """
INSERT INTO outcomes(transaction_id, account, op_kind, status, balance_cents,
                     event_time, tx_type, is_fraud, ingest_time_ms, settle_time_ms)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (transaction_id, account, op_kind) DO NOTHING;
"""

UPSERT_ALERT = """
INSERT INTO aml_alerts(alert_id, pattern, accounts, amount_cents, event_time, detail)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (alert_id) DO NOTHING;
"""


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    metrics_port = int(os.environ.get("METRICS_PORT", "8000"))
    start_http_server(metrics_port)

    conn = connect_pg()
    cur = conn.cursor()

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "pix-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([OUT_TOPIC, ALERT_TOPIC])
    print(f"[consumer] iniciado; métricas em :{metrics_port}", flush=True)

    bal_batch, out_batch, alert_batch = [], [], []
    last_commit = time.time()

    def flush():
        if bal_batch:
            execute_batch(cur, UPSERT_BAL, bal_batch)
            bal_batch.clear()
        if out_batch:
            execute_batch(cur, UPSERT_OUT, out_batch)
            out_batch.clear()
        if alert_batch:
            execute_batch(cur, UPSERT_ALERT, alert_batch)
            alert_batch.clear()
        conn.commit()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_commit > 2:
                    flush(); last_commit = time.time()
                continue
            if msg.error():
                sys.stderr.write(f"[consumer] erro: {msg.error()}\n")
                continue
            topic = msg.topic()
            try:
                d = json.loads(msg.value())
            except Exception:
                continue

            if topic == OUT_TOPIC:
                status = d.get("status", "?")
                OUTCOMES.labels(status=status).inc()
                ing = int(d.get("ingest_time_ms", 0) or 0)
                stl = int(d.get("settle_time_ms", 0) or 0)
                if ing > 0 and stl >= ing:
                    LATENCY.observe(stl - ing)
                ok = order_key(d["event_time"], d["transaction_id"], d["op_kind"])
                if status in ("SETTLED", "REJECTED"):
                    bal_batch.append((d["account"], d["balance_cents"], ok))
                out_batch.append((
                    d["transaction_id"], d["account"], d["op_kind"], status,
                    d["balance_cents"], d["event_time"], d.get("tx_type", ""),
                    d.get("is_fraud", 0), ing or None, stl or None))
            elif topic == ALERT_TOPIC:
                ALERTS.labels(pattern=d.get("pattern", "?")).inc()
                alert_batch.append((
                    d["alert_id"], d["pattern"], d["accounts"],
                    d.get("amount_cents"), d["event_time"], d.get("detail", "")))

            if len(bal_batch) + len(out_batch) + len(alert_batch) >= 1000 \
                    or time.time() - last_commit > 2:
                flush(); last_commit = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        flush()
        consumer.close()
        cur.close()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
