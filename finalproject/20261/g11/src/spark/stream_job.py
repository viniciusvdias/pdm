#!/usr/bin/env python3
"""
PySpark Structured Streaming (NYC TLC)
======================================

Consome o stream canonico do Kafka (topico `taxi_trips_stream`), limpa anomalias,
calcula duas agregacoes COM SHUFFLE, enriquece com a tabela de zonas da TLC e
grava o resultado em Parquet com checkpoint.

Pipeline
--------
1.  readStream do Kafka (bootstrap kafka:9092) -> coluna `value` (bytes).
2.  Parse do JSON no SCHEMA CANONICO (8 campos).
3.  withWatermark em `pickup_datetime` (EVENT TIME) -> tolera atraso/desordem.
4.  Filtro de anomalias:
       - trip_duration_s <= 0           (viagens de duracao invalida)
       - fare_amount <= 0 quando != null (tarifas invalidas; fhv tem fare null,
         e mantido pois null nao e <= 0).
5.  Duas agregacoes (cada uma faz SHUFFLE no groupBy):
       (a) pico diario de corridas:  groupBy(window(pickup_datetime, "1 day"))
       (b) tempo medio de viagem por zona de embarque:
           groupBy(pu_location_id, window) -> avg(trip_duration_s)
           ENRIQUECIDO via stream-static JOIN com taxi_zone_lookup.csv.
6.  writeStream em Parquet (append) para /output, checkpoint em /checkpoints.

Por que duas queries de escrita?
--------------------------------
O Structured Streaming nao permite multiplas agregacoes encadeadas numa unica
query em append mode; cada agregacao com watermark vira uma StreamingQuery
independente, com seu proprio diretorio de saida e checkpoint. Ambas leem do
MESMO readStream (Spark reaproveita a fonte Kafka).

Por que append mode?
--------------------
Append em agregacao com watermark emite cada janela UMA vez, quando o watermark
ultrapassa o fim da janela (resultado final, imutavel) — exatamente o que o
Parquet (sink append-only) exige. Trade-off: a saida so aparece depois que o
watermark fecha as janelas (ver nota no final do arquivo).

Execucao: ver spark/spark-submit.sh (empacota org.apache.spark:spark-sql-kafka).
"""

import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType,
)

# --------------------------------------------------------------------------- #
# Schema canonico do evento (espelha producer/producer.py CANONICAL_FIELDS).
# Datas vem como STRING ISO-8601 no JSON e sao convertidas para timestamp.
# --------------------------------------------------------------------------- #
CANONICAL_SCHEMA = StructType([
    StructField("service_type",     StringType(),  True),
    StructField("pickup_datetime",  StringType(),  True),   # ISO-8601 -> ts
    StructField("dropoff_datetime", StringType(),  True),
    StructField("pu_location_id",   IntegerType(), True),
    StructField("do_location_id",   IntegerType(), True),
    StructField("trip_distance",    DoubleType(),  True),
    StructField("fare_amount",      DoubleType(),  True),   # null p/ fhv
    StructField("trip_duration_s",  LongType(),    True),
])


# --------------------------------------------------------------------------- #
# Coletor de metricas para o benchmark.
#
# O Structured Streaming NAO expoe StreamingQueryProgress no endpoint REST
# /api/v1/applications/.../streaming (esse e exclusivo do DStreams legado). A
# fonte canonica e o objeto StreamingQueryProgress emitido a cada micro-batch.
# Capturamos via StreamingQueryListener e gravamos um JSONL (1 linha por batch)
# que o parser (benchmarks/parse_metrics.py) le para calcular:
#   - THROUGHPUT          -> numInputRows / (durationMs.triggerExecution/1000)
#                            (alias do processedRowsPerSecond do progress)
#   - MICRO-BATCH LATENCY -> durationMs.triggerExecution (ms por micro-batch)
# Cada linha ja carrega esses campos crus do progress, para que o calculo de
# media/desvio fique no parser.
# --------------------------------------------------------------------------- #
class ProgressFileListener(StreamingQueryListener):
    """Grava cada StreamingQueryProgress como JSONL e RASTREIA atividade.

    Alem de gravar as metricas, o listener mantem contadores em memoria
    (total_rows, last_data_ts) que o loop principal le SEM chamar py4j. Isso
    evita o deadlock do gateway py4j que ocorria quando a thread principal
    chamava q.lastProgress enquanto o listener disparava callbacks (ambos
    disputam o gateway single-thread -> job travava sem encerrar).
    """

    def __init__(self, metrics_file):
        self._path = metrics_file
        # Estado compartilhado, lido pelo loop principal (puro Python, sem py4j).
        self.total_rows = 0
        self.last_data_ts = None     # epoch do ultimo micro-batch com dados
        self.any_progress = False    # ja houve QUALQUER progresso (mesmo vazio)

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        import time as _time
        self.any_progress = True
        p = event.progress
        # p.json e a serializacao oficial do StreamingQueryProgress; guardamos o
        # objeto inteiro mais alguns campos achatados para facilitar o parser.
        try:
            raw = json.loads(p.json)
        except Exception:
            raw = {}
        durations = raw.get("durationMs", {}) or {}
        rows = raw.get("numInputRows") or 0
        if rows > 0:
            self.total_rows += rows
            self.last_data_ts = _time.time()
        record = {
            "query": raw.get("name") or p.name,
            "batchId": raw.get("batchId"),
            "timestamp": raw.get("timestamp"),
            "numInputRows": rows,
            "inputRowsPerSecond": raw.get("inputRowsPerSecond"),
            "processedRowsPerSecond": raw.get("processedRowsPerSecond"),
            # Latencia do micro-batch: tempo total de execucao do trigger (ms).
            "triggerExecutionMs": durations.get("triggerExecution"),
            "batchDurationMs": durations.get("triggerExecution"),
            "addBatchMs": durations.get("addBatch"),
        }
        # Grava o JSONL apenas se ha um arquivo de metricas configurado. Quando o
        # listener existe so para o --idle-stop (sem --metrics-file), self._path e
        # None e nao escrevemos nada — apenas rastreamos total_rows/last_data_ts.
        if self._path:
            with open(self._path, "a") as fh:
                fh.write(json.dumps(record) + "\n")

    def onQueryTerminated(self, event):
        pass

    # Spark 3.5 tambem dispara onQueryIdle em algumas versoes; tolere-o.
    def onQueryIdle(self, event):  # pragma: no cover - depende da versao
        pass


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Particoes de shuffle: alinhe ao paralelismo do cluster. Mantemos baixo
        # (default 200 e exagero para um smoke test) para reduzir overhead.
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, bootstrap, topic, starting_offsets):
    """readStream do Kafka -> DataFrame com o evento canonico ja parseado."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        # failOnDataLoss=false: tolera truncamento/retencao do topico no smoke test.
        .option("failOnDataLoss", "false")
        .load()
    )

    # value (bytes) -> JSON string -> colunas canonicas.
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), CANONICAL_SCHEMA).alias("e"))
        .select("e.*")
        # ISO-8601 string -> timestamp (event time).
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
    )
    return parsed


def clean(df, watermark_delay):
    """Watermark + filtro de anomalias."""
    return (
        df
        # Descarta eventos sem event-time (nao da para janela-los).
        .filter(F.col("pickup_datetime").isNotNull())
        # Watermark no EVENT TIME: eventos mais atrasados que isto sao descartados
        # e permite que o estado das janedas seja liberado.
        .withWatermark("pickup_datetime", watermark_delay)
        # Anomalia 1: duracao nao-positiva.
        .filter(F.col("trip_duration_s") > 0)
        # Anomalia 2: tarifa <= 0 QUANDO nao-nula. fhv tem fare null e e mantido
        # (null nao satisfaz <= 0, entao a condicao abaixo o preserva).
        .filter(F.col("fare_amount").isNull() | (F.col("fare_amount") > 0))
    )


def load_zone_lookup(spark, path):
    """Tabela estatica de zonas da TLC (taxi_zone_lookup.csv).

    LE O CSV NO DRIVER (via pandas) e materializa o DataFrame a partir das linhas
    em memoria — NAO via `spark.read.csv`. No cluster multi-host (Swarm) so o
    node-infra tem o bind-mount /zones; se usassemos `spark.read.csv`, a leitura
    viraria uma task agendada num executor (VM worker) que NAO tem o arquivo,
    falhando com "File file:/zones/... does not exist". A tabela e minuscula
    (~265 zonas), entao ler no driver e broadcast e o caminho correto e barato.
    """
    import csv as _csv

    schema = StructType([
        StructField("location_id", IntegerType()),
        StructField("borough", StringType()),
        StructField("zone", StringType()),
        StructField("service_zone", StringType()),
    ])

    rows = []
    with open(path, newline="") as fh:
        for r in _csv.DictReader(fh):
            try:
                loc = int(r["LocationID"])
            except (KeyError, ValueError, TypeError):
                continue
            rows.append((loc, r.get("Borough"), r.get("Zone"), r.get("service_zone")))

    # broadcast(): forca o broadcast join (tabela pequena), evitando shuffle.
    return F.broadcast(spark.createDataFrame(rows, schema))


def agg_daily_peaks(clean_df):
    """(a) Pico diario de corridas: contagem por janela de 1 dia (SHUFFLE)."""
    return (
        clean_df
        .groupBy(F.window("pickup_datetime", "1 day").alias("day_window"))
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .select(
            F.col("day_window.start").alias("window_start"),
            F.col("day_window.end").alias("window_end"),
            "trip_count",
        )
    )


def agg_avg_duration_by_zone(clean_df, zones):
    """(b) Tempo medio de viagem por zona de embarque (SHUFFLE + stream-static JOIN).

    Agrega por (pu_location_id, janela de 1 dia) e enriquece com o nome da zona.
    Inclui a janela para que o append mode tenha um criterio de finalizacao via
    watermark (groupBy so por chave nao-temporal nao e suportado em append).
    """
    by_zone = (
        clean_df
        .groupBy(
            F.window("pickup_datetime", "1 day").alias("day_window"),
            F.col("pu_location_id"),
        )
        .agg(
            F.avg("trip_duration_s").alias("avg_trip_duration_s"),
            F.count(F.lit(1)).alias("trip_count"),
        )
    )
    # Stream-static JOIN: enriquece com borough/zone (suportado em append).
    enriched = (
        by_zone.join(zones, by_zone.pu_location_id == zones.location_id, "left")
        .select(
            F.col("day_window.start").alias("window_start"),
            F.col("day_window.end").alias("window_end"),
            F.col("pu_location_id"),
            F.col("borough"),
            F.col("zone"),
            F.round("avg_trip_duration_s", 1).alias("avg_trip_duration_s"),
            F.col("trip_count"),
        )
    )
    return enriched


def start_parquet_query(df, name, out_path, ckpt_path, trigger_interval):
    writer = (
        df.writeStream
        .queryName(name)
        .format("parquet")
        .outputMode("append")
        .option("path", out_path)
        .option("checkpointLocation", ckpt_path)
    )
    if trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def parse_args():
    p = argparse.ArgumentParser(description="PySpark Structured Streaming — NYC TLC")
    p.add_argument("--bootstrap", default="kafka:9092",
                   help="kafka.bootstrap.servers (default: kafka:9092)")
    p.add_argument("--topic", default="taxi_trips_stream")
    p.add_argument("--zones-csv", default="/zones/taxi_zone_lookup.csv")
    p.add_argument("--output-dir", default="/output")
    p.add_argument("--checkpoint-dir", default="/checkpoints")
    p.add_argument("--watermark-delay", default="2 hours",
                   help="atraso tolerado no event-time (withWatermark)")
    p.add_argument("--starting-offsets", default="earliest",
                   help="earliest|latest (default: earliest, processa o backlog)")
    p.add_argument("--trigger", default="10 seconds",
                   help="intervalo do micro-batch (vazio = o mais rapido possivel)")
    p.add_argument("--await-timeout", type=int, default=0,
                   help="segundos a aguardar antes de encerrar (0 = roda indefinido). "
                        "Util para smoke test: encerra apos drenar o backlog.")
    p.add_argument("--metrics-file", default="",
                   help="se definido, grava 1 JSON por micro-batch (StreamingQueryProgress) "
                        "neste arquivo (insumo do benchmark).")
    p.add_argument("--idle-stop", type=int, default=0,
                   help="encerra apos N ciclos de poll sem novas linhas de entrada "
                        "(0 = desliga; usado pelo benchmark p/ parar quando o backlog drena).")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("nyc-tlc-stream")

    # Coletor de metricas do benchmark (StreamingQueryProgress -> JSONL).
    # Tambem e o que rastreia atividade para o --idle-stop; por isso o criamos
    # quando HA metrics-file OU quando --idle-stop foi pedido (ex.: bin/run.sh,
    # que passa --idle-stop sem --metrics-file). Sem arquivo, ele apenas mede
    # total_rows/last_data_ts (nao grava JSONL).
    metrics_listener = None
    if args.metrics_file or args.idle_stop > 0:
        metrics_path = args.metrics_file or None
        if metrics_path:
            # Trunca o arquivo no inicio do run (cada run do benchmark = limpo).
            open(metrics_path, "w").close()
        metrics_listener = ProgressFileListener(metrics_path)
        spark.streams.addListener(metrics_listener)
        if metrics_path:
            print(f"[stream] metricas por micro-batch -> {metrics_path}")
        else:
            print("[stream] listener de atividade ativo (--idle-stop)")

    parsed = read_kafka_stream(spark, args.bootstrap, args.topic, args.starting_offsets)
    cleaned = clean(parsed, args.watermark_delay)
    zones = load_zone_lookup(spark, args.zones_csv)

    # (a) Pico diario de corridas.
    daily = agg_daily_peaks(cleaned)
    q_daily = start_parquet_query(
        daily, "daily_peaks",
        f"{args.output_dir}/daily_peaks",
        f"{args.checkpoint_dir}/daily_peaks",
        args.trigger,
    )

    # (b) Tempo medio por zona (com join de zonas).
    by_zone = agg_avg_duration_by_zone(cleaned, zones)
    q_zone = start_parquet_query(
        by_zone, "avg_duration_by_zone",
        f"{args.output_dir}/avg_duration_by_zone",
        f"{args.checkpoint_dir}/avg_duration_by_zone",
        args.trigger,
    )

    print(f"[stream] queries ativas: {[q.name for q in spark.streams.active]}")
    print(f"[stream] saida em {args.output_dir}, checkpoint em {args.checkpoint_dir}")

    if args.await_timeout > 0 or args.idle_stop > 0:
        # Smoke test / benchmark: roda por um tempo fixo, depois para gracefulmente.
        # IMPORTANTE: em append mode a saida so materializa quando o watermark
        # ultrapassa o fim das janelas. Para garantir Parquet num smoke test
        # curto, use --watermark-delay "0 seconds" (ver spark-submit.sh).
        #
        # --idle-stop N: encerra mais cedo se, por N polls seguidos, NENHUMA query
        # processou novas linhas (numInputRows==0). No benchmark isto
        # garante que o run termine assim que o volume fixo de mensagens drena, em
        # vez de esperar o --await-timeout inteiro (mantemos o timeout como teto).
        # --idle-stop SOZINHO (sem --await-timeout, o caso do bin/run.sh) tambem
        # entra aqui: usamos um teto de seguranca de 1h para o `deadline`, mas na
        # pratica o encerramento por inatividade dispara muito antes.
        import time
        AWAIT_CEILING_S = 3600  # teto de seguranca quando so --idle-stop e passado
        timeout_s = args.await_timeout if args.await_timeout > 0 else AWAIT_CEILING_S
        deadline = time.time() + timeout_s
        # Encerramento por INATIVIDADE baseado em RELOGIO, lendo SO o estado do
        # listener (sem py4j na thread principal -> sem deadlock do gateway).
        # idle_stop (em "polls" de 5s) vira uma janela de silencio em segundos.
        poll_s = 5
        idle_window_s = max(args.idle_stop, 1) * poll_s if args.idle_stop > 0 else 0
        last_total = -1
        while time.time() < deadline:
            time.sleep(poll_s)
            total = metrics_listener.total_rows if metrics_listener else 0
            if total != last_total:
                print(f"[stream] linhas processadas (acum.): {total:,}")
                last_total = total

            if idle_window_s > 0 and metrics_listener is not None:
                # So encerra DEPOIS de ter visto dados (last_data_ts setado) e de
                # passar a janela de silencio sem novas linhas.
                ldt = metrics_listener.last_data_ts
                if ldt is not None and (time.time() - ldt) >= idle_window_s:
                    print(f"[stream] backlog drenado ({idle_window_s}s sem novas "
                          f"linhas) — encerrando. Total: {total:,} linhas.")
                    break
        else:
            print("[stream] timeout atingido — encerrando queries...")
        # Remove o listener ANTES de parar as queries: evita o callback
        # onQueryTerminated disparar para um gateway py4j ja em teardown (ruido
        # "Connection reset" no log — inofensivo, mas confuso). As metricas de
        # micro-batch ja foram todas capturadas via onQueryProgress.
        if metrics_listener is not None:
            try:
                spark.streams.removeListener(metrics_listener)
            except Exception:
                pass
        for q in spark.streams.active:
            q.stop()
        for q in (q_daily, q_zone):
            try:
                q.awaitTermination(30)
            except Exception:
                pass
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
