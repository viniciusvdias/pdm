import os
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, expr, window, to_timestamp, from_unixtime,
    coalesce, lit, when, regexp_replace, trim, concat_ws,
)
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType
from sqlite_store import SQLiteStore, DEFAULT_DB_PATH
from classifier import classify_udf, preload_model

# --- CONFIGURAÇÕES DE AMBIENTE ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'wikimediaRecentchange')
WINDOW_DURATION = os.environ.get('WINDOW_DURATION', '5 minutes')
WINDOW_SLIDE_DURATION = os.environ.get('WINDOW_SLIDE_DURATION', WINDOW_DURATION)

# Backpressure: máximo de registros lidos do Kafka por micro-batch.
MAX_OFFSETS_PER_TRIGGER = os.environ.get('MAX_OFFSETS_PER_TRIGGER', '5000')

# Checkpoint
CHECKPOINT_LOCATION = os.environ.get('CHECKPOINT_LOCATION', '/checkpoint')

# Persistência SQLite
SQLITE_DB_PATH = os.environ.get('SQLITE_DB_PATH', DEFAULT_DB_PATH)


def run_batch_with_metrics(batch_name, batch_df, batch_id, handler):
    started_at = time.perf_counter()
    row_count = batch_df.count()
    print(f"[batch:{batch_name}] batch_id={batch_id} rows_in_batch={row_count}", flush=True)
    handler(batch_df, batch_id)
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    throughput = (row_count / (elapsed_ms / 1000.0)) if elapsed_ms > 0 else 0.0
    print(
        f"[batch:{batch_name}] batch_id={batch_id} duration_ms={elapsed_ms:.2f} "
        f"throughput_eps={throughput:.2f}",
        flush=True,
    )

# Mostra e persiste apenas a janela mais recente de cada micro-batch.
def show_latest_window(batch_df, batch_id):
    latest_start = batch_df.agg({"win_start": "max"}).first()[0]
    if latest_start is None:
        return

    latest = batch_df.filter(col("win_start") == latest_start)
    latest.orderBy(col("quantidade").desc()) \
        .drop("win_start", "win_end") \
        .show(50, truncate=False)

    rows = latest.collect()
    if not rows:
        return

    max_count = max(r["quantidade"] for r in rows) or 1
    try:
        with SQLiteStore(SQLITE_DB_PATH) as store:
            for row in rows:
                store.insert_metric({
                    "window_start": row["win_start"],
                    "window_end": row["win_end"],
                    "category": row["categoria"],
                    "count": int(row["quantidade"]),
                    "trend_score": round(row["quantidade"] / max_count * 100.0, 2),
                })
    except Exception as e:
        print(f"[SQLite] Error writing window_metrics: {e}", flush=True)


# Persiste uma amostra dos eventos individuais classificados por micro-batch.
def write_events_sample(batch_df, batch_id):
    rows = batch_df.filter(col("title").isNotNull()) \
        .orderBy(col("event_time").desc()) \
        .limit(100) \
        .select("event_time", "title", "category") \
        .collect()
    if not rows:
        return
    try:
        with SQLiteStore(SQLITE_DB_PATH) as store:
            for row in rows:
                store.insert_event({
                    "timestamp": row["event_time"],
                    "title": row["title"],
                    "category": row["category"],
                })
    except Exception as e:
        print(f"[SQLite] Error writing events: {e}", flush=True)

def clean_text_column():
    title_c = coalesce(col("title"), lit(""))
    comment_c = coalesce(col("comment"), lit(""))
    parsed_c = coalesce(col("parsedcomment"), lit(""))
    ns_c = coalesce(col("namespace"), lit(0))

    # Itens Wikidata (título "Q123..."): usa o parsedcomment sem tags HTML.
    is_wikidata = title_c.rlike(r"^Q\d+") & (parsed_c != "")
    wikidata_text = regexp_replace(parsed_c, r"<[^>]*>", "")

    # Remove prefixos File:/Category: quando namespace é 6 (File) ou 14 (Category).
    title_stripped = when(
        ns_c.isin(6, 14),
        regexp_replace(title_c, r"^(File:|Category:|Categoria:)", ""),
    ).otherwise(title_c)

    # Resolve wiki-links [[alvo|texto]] -> texto e remove seções /* ... */.
    clean_comment = trim(
        regexp_replace(
            regexp_replace(comment_c, r"\[\[(.*\|)?(.*?)\]\]", "$2"),
            r"/\*.*?\*/",
            "",
        )
    )

    return when(is_wikidata, wikidata_text).otherwise(
        trim(concat_ws(" ", title_stripped, clean_comment))
    )

# Força o download do modelo no Driver ANTES de iniciar o Spark Streaming.
preload_model()

# --- INICIALIZAÇÃO DO SPARK ---
spark = SparkSession.builder \
    .appName("WikimediaSemanticClassifier") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Evita logs de warning KAFKA-1894;

# --- SCHEMA ---
schema = StructType([
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("parsedcomment", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# --- STREAMING ---
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER) \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_cleaned = df_parsed.withColumn("cleaned_text", clean_text_column())
df_timed = df_cleaned.withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

# Aplica a IA e quebra o resultado em Categoria e Confiança
df_inference = df_timed.withColumn("inference_raw", classify_udf(col("cleaned_text"))) \
    .withColumn("category", expr("split(inference_raw, '\\\\|')[0]")) \
    .withColumn("confidence", expr("split(inference_raw, '\\\\|')[1]").cast("float"))

df_windowed = df_inference \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE_DURATION).alias("time_window"),
        col("category")
    ).count()

df_result = df_windowed.select(
    col("time_window.start").alias("win_start"),
    col("time_window.end").alias("win_end"),
    expr("concat(date_format(time_window.start, 'HH:mm'), '-', date_format(time_window.end, 'HH:mm'))").alias("janela"),
    col("category").alias("categoria"),
    col("count").alias("quantidade")
)

query = df_result \
    .writeStream \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .foreachBatch(lambda batch_df, batch_id: run_batch_with_metrics(
        "windows",
        batch_df,
        batch_id,
        show_latest_window,
    )) \
    .start()

events_query = df_inference \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "/events") \
    .foreachBatch(lambda batch_df, batch_id: run_batch_with_metrics(
        "events",
        batch_df,
        batch_id,
        write_events_sample,
    )) \
    .start()

spark.streams.awaitAnyTermination()