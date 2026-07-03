#!/usr/bin/env python3
"""
Ingestao de Dados (Kafka Firehose)
==================================

Producer de alto throughput que le os arquivos Parquet do NYC TLC Trip Record
Data (Yellow, Green, FHV e FHVHV), normaliza cada linha para um *evento
canonico* unico e dispara para o topico Kafka `taxi_trips_stream`.

Por que normalizar?
-------------------
Os quatro datasets tem schemas heterogeneos (colunas de event-time diferentes:
`tpep_pickup_datetime`, `lpep_pickup_datetime`, `pickup_datetime`). Para que o
Spark Structured Streaming consuma um stream uniforme com um unico watermark,
mapeamos todos para o schema canonico definido em CANONICAL_FIELDS.

Alinhamento com os slides da disciplina (9-pdm, Kafka):
- Producer.send(topic, key, data): aqui `key = service_type`, de modo que o
  particionamento por hash co-localiza eventos do mesmo tipo na mesma particao.
- Kafka garante ordem *apenas dentro de uma particao*. Portanto NAO tentamos
  garantir ordem global no broker; o tratamento de eventos fora de ordem fica a
  cargo do *watermark* do Spark (slides 8-pdm). O producer faz, no maximo, uma
  ordenacao best-effort por batch (--strict-global-sort para ordenar por arquivo).
- acks=1 + compression lz4 + batching agressivo => vazao maxima
  (delivery semantics: at-least/at-most-once, conforme slide "Delivery semantics").

Uso:
    python producer.py                         # stream completo
    python producer.py --max-records 100000    # smoke test
    python producer.py --strict-global-sort    # ordena cada arquivo por event-time
    python producer.py --rate-limit 50000      # simula tempo real (msgs/s)
"""

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime

import pyarrow.parquet as pq
from confluent_kafka import Producer

# --------------------------------------------------------------------------- #
# Configuracao de schema: mapeamento de cada dataset -> evento canonico
# --------------------------------------------------------------------------- #

# Campos do evento canonico (a ordem aqui e apenas documental).
CANONICAL_FIELDS = (
    "service_type",      # str: fhvhv | yellow | green | fhv
    "pickup_datetime",   # str ISO-8601 (EVENT TIME -> watermark no Spark)
    "dropoff_datetime",  # str ISO-8601
    "pu_location_id",    # int | None
    "do_location_id",    # int | None
    "trip_distance",     # float | None (milhas)
    "fare_amount",       # float | None
    "trip_duration_s",   # int | None (dropoff - pickup, em segundos)
)

# Para cada service_type, qual coluna do parquet preenche cada campo canonico.
# `None` significa "coluna inexistente neste dataset" (vira null no JSON).
SCHEMA_MAP = {
    "yellow": {
        "pickup_col":   "tpep_pickup_datetime",
        "dropoff_col":  "tpep_dropoff_datetime",
        "pu_col":       "PULocationID",
        "do_col":       "DOLocationID",
        "distance_col": "trip_distance",
        "fare_col":     "fare_amount",
    },
    "green": {
        "pickup_col":   "lpep_pickup_datetime",
        "dropoff_col":  "lpep_dropoff_datetime",
        "pu_col":       "PULocationID",
        "do_col":       "DOLocationID",
        "distance_col": "trip_distance",
        "fare_col":     "fare_amount",
    },
    "fhvhv": {
        "pickup_col":   "pickup_datetime",
        "dropoff_col":  "dropoff_datetime",
        "pu_col":       "PULocationID",
        "do_col":       "DOLocationID",
        "distance_col": "trip_miles",
        "fare_col":     "base_passenger_fare",
    },
    "fhv": {
        "pickup_col":   "pickup_datetime",
        "dropoff_col":  "dropOff_datetime",
        "pu_col":       "PUlocationID",
        "do_col":       "DOlocationID",
        "distance_col": None,   # FHV nao traz distancia
        "fare_col":     None,   # FHV nao traz tarifa
    },
}

TOPIC = os.environ.get("KAFKA_TOPIC", "taxi_trips_stream")


def infer_service_type(filename: str) -> str | None:
    """Infere o service_type a partir do nome do arquivo (ex.: fhvhv_tripdata_*).

    A ordem importa: 'fhvhv' deve ser testado antes de 'fhv'.
    """
    name = os.path.basename(filename).lower()
    for stype in ("fhvhv", "yellow", "green", "fhv"):
        if name.startswith(stype):
            return stype
    return None


# --------------------------------------------------------------------------- #
# Conversao de tipos / serializacao
# --------------------------------------------------------------------------- #

def _iso(value) -> str | None:
    """Converte timestamp (pandas/py datetime) para ISO-8601, ou None."""
    if value is None:
        return None
    # pyarrow entrega datetime nativos; valores nulos viram None acima.
    if isinstance(value, datetime):
        return value.isoformat()
    # pandas.Timestamp tem isoformat tambem; fallback para str.
    try:
        return value.isoformat()
    except AttributeError:
        return str(value)


def _num(value):
    """Normaliza numericos: NaN/None -> None, senao mantem o valor."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _to_event(row: dict, stype: str, cfg: dict) -> dict:
    """Transforma uma linha (dict coluna->valor) no evento canonico."""
    pickup = row.get(cfg["pickup_col"])
    dropoff = row.get(cfg["dropoff_col"])

    duration_s = None
    if isinstance(pickup, datetime) and isinstance(dropoff, datetime):
        duration_s = int((dropoff - pickup).total_seconds())

    distance = _num(row.get(cfg["distance_col"])) if cfg["distance_col"] else None
    fare = _num(row.get(cfg["fare_col"])) if cfg["fare_col"] else None
    pu = _num(row.get(cfg["pu_col"]))
    do = _num(row.get(cfg["do_col"]))

    return {
        "service_type": stype,
        "pickup_datetime": _iso(pickup),
        "dropoff_datetime": _iso(dropoff),
        "pu_location_id": int(pu) if pu is not None else None,
        "do_location_id": int(do) if do is not None else None,
        "trip_distance": float(distance) if distance is not None else None,
        "fare_amount": float(fare) if fare is not None else None,
        "trip_duration_s": duration_s,
    }


# --------------------------------------------------------------------------- #
# Producer Kafka
# --------------------------------------------------------------------------- #

def build_producer(bootstrap: str) -> Producer:
    """Cria o Producer confluent-kafka (librdkafka) tunado para vazao maxima."""
    conf = {
        "bootstrap.servers": bootstrap,
        # --- Tuning de throughput ---
        "compression.type": "lz4",          # compressao rapida, otima p/ JSON
        "linger.ms": 50,                     # acumula antes de enviar (batching)
        "batch.num.messages": 100_000,       # mensagens por batch
        "queue.buffering.max.messages": 2_000_000,
        "queue.buffering.max.kbytes": 1_048_576,  # 1 GiB de buffer de saida
        "acks": "1",                         # at-least/at-most-once -> vazao
        # Mantem a fila local cheia sem estourar:
        "message.max.bytes": 2_097_152,
    }
    return Producer(conf)


def delivery_report(err, msg):
    """Callback chamado apenas em erro (evita overhead em sucesso)."""
    if err is not None:
        sys.stderr.write(f"[ERRO] entrega falhou: {err}\n")


def iter_rows(path: str, cfg: dict, strict_sort: bool):
    """Itera linhas de um parquet como dicts coluna->valor.

    Le em *row groups* (via iter_batches) para nao carregar o arquivo inteiro na
    memoria. Com --strict-global-sort, carrega o arquivo todo e ordena por
    event-time (custoso, didatico).
    """
    pf = pq.ParquetFile(path)
    # So precisamos das colunas usadas pelo mapeamento (reduz I/O e memoria).
    wanted = [
        cfg[k] for k in ("pickup_col", "dropoff_col", "pu_col", "do_col",
                         "distance_col", "fare_col")
        if cfg[k] is not None
    ]
    # Garante colunas existentes no schema do arquivo.
    available = set(pf.schema_arrow.names)
    cols = [c for c in wanted if c in available]

    if strict_sort:
        table = pf.read(columns=cols)
        df = table.to_pandas()
        if cfg["pickup_col"] in df.columns:
            df = df.sort_values(cfg["pickup_col"], kind="mergesort")
        for record in df.to_dict("records"):
            yield record
        return

    for batch in pf.iter_batches(batch_size=50_000, columns=cols):
        df = batch.to_pandas()
        # Ordenacao best-effort por batch (Kafka so garante ordem por particao).
        if cfg["pickup_col"] in df.columns:
            df = df.sort_values(cfg["pickup_col"], kind="mergesort")
        for record in df.to_dict("records"):
            yield record


def run(args):
    data_dir = args.data_dir
    files = sorted(
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.endswith(".parquet")
    )
    if not files:
        sys.exit(f"Nenhum .parquet encontrado em {data_dir!r}")

    producer = build_producer(args.bootstrap)

    total = 0
    skipped_files = 0
    t_start = time.monotonic()
    t_last = t_start
    last_count = 0

    # (stype, cfg, path) por arquivo valido; arquivos desconhecidos sao ignorados.
    sources = []
    for path in files:
        stype = infer_service_type(path)
        if stype is None:
            sys.stderr.write(f"[aviso] ignorando arquivo desconhecido: {path}\n")
            skipped_files += 1
            continue
        sources.append((stype, SCHEMA_MAP[stype], path))

    print(f"[producer] topico={TOPIC} bootstrap={args.bootstrap}")
    print(f"[producer] {len(sources)} arquivo(s) valido(s) em {data_dir}"
          f"{' | modo=interleave' if args.interleave else ''}")

    if args.interleave:
        # Round-robin entre os arquivos: garante que um smoke test limitado
        # (--max-records) amostre TODOS os service_type, nao so o primeiro.
        iterators = [
            (stype, iter_rows(path, cfg, args.strict_global_sort))
            for stype, cfg, path in sources
        ]
        row_stream = _round_robin(iterators)
    else:
        # Sequencial: processa um arquivo por vez (vazao maxima por arquivo).
        def _sequential():
            for stype, cfg, path in sources:
                print(f"[producer] -> {os.path.basename(path)} (service_type={stype})")
                for row in iter_rows(path, cfg, args.strict_global_sort):
                    yield stype, cfg, row
        row_stream = _sequential()

    for stype, cfg, row in row_stream:
        event = _to_event(row, stype, cfg)
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
        # key = service_type => particionamento por hash co-localiza tipos.
        while True:
            try:
                producer.produce(
                    TOPIC,
                    key=stype,
                    value=payload,
                    on_delivery=delivery_report,
                )
                break
            except BufferError:
                # Fila local cheia: drena callbacks e tenta de novo.
                producer.poll(0.1)

        producer.poll(0)  # serve callbacks pendentes sem bloquear
        total += 1

        # --- Relatorio de throughput periodico ---
        if total % args.report_every == 0:
            now = time.monotonic()
            inst = (total - last_count) / max(now - t_last, 1e-9)
            avg = total / max(now - t_start, 1e-9)
            print(f"[producer] {total:>12,} msgs | "
                  f"inst={inst:>10,.0f}/s | media={avg:>10,.0f}/s")
            t_last, last_count = now, total

        # --- Rate limit opcional (simula chegada em tempo real) ---
        if args.rate_limit:
            expected = total / args.rate_limit
            elapsed = time.monotonic() - t_start
            if elapsed < expected:
                time.sleep(expected - elapsed)

        if args.max_records and total >= args.max_records:
            print(f"[producer] limite --max-records={args.max_records} atingido")
            _finish(producer, total, t_start, skipped_files)
            return

    _finish(producer, total, t_start, skipped_files)


def _round_robin(iterators):
    """Itera (stype, cfg, row) alternando entre varios iteradores de arquivos.

    `iterators` e uma lista de (stype, iterador-de-rows). Os iteradores de rows
    ja embutem o cfg via iter_rows; recuperamos o cfg pelo SCHEMA_MAP[stype].
    Quando um iterador se esgota, ele e removido do rodizio.
    """
    active = [(stype, SCHEMA_MAP[stype], it) for stype, it in iterators]
    while active:
        next_active = []
        for stype, cfg, it in active:
            try:
                row = next(it)
            except StopIteration:
                continue
            yield stype, cfg, row
            next_active.append((stype, cfg, it))
        active = next_active


def _finish(producer, total, t_start, skipped_files):
    print("[producer] flush final...")
    producer.flush(30)
    elapsed = time.monotonic() - t_start
    print(f"[producer] CONCLUIDO: {total:,} mensagens em {elapsed:,.1f}s "
          f"(media {total / max(elapsed, 1e-9):,.0f}/s) | "
          f"arquivos ignorados: {skipped_files}")


def parse_args():
    p = argparse.ArgumentParser(description="NYC TLC Kafka firehose producer")
    p.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        help="bootstrap.servers do Kafka (default: env KAFKA_BOOTSTRAP ou localhost:29092)",
    )
    p.add_argument(
        "--data-dir",
        default=os.environ.get("DATA_DIR", "./data"),
        help="diretorio com os .parquet (default: env DATA_DIR ou ./data)",
    )
    p.add_argument("--max-records", type=int, default=0,
                   help="limita o total de mensagens (0 = sem limite; smoke test)")
    p.add_argument("--rate-limit", type=int, default=0,
                   help="msgs/s alvo (0 = vazao maxima)")
    p.add_argument("--report-every", type=int, default=200_000,
                   help="intervalo (em msgs) do relatorio de throughput")
    p.add_argument("--strict-global-sort", action="store_true",
                   help="ordena cada arquivo inteiro por event-time (custoso, didatico)")
    p.add_argument("--interleave", action="store_true",
                   help="round-robin entre todos os arquivos (smoke test amostra "
                        "todos os service_type, nao so o primeiro)")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())
