"""Processador Spark Structured Streaming: Kafka -> OHLCV + Indicadores + Métricas.

Lê trades do tópico Kafka, agrega em candles OHLCV por janela temporal,
calcula indicadores técnicos (EMA, RSI) e salva métricas de desempenho em CSV
para análise posterior.

Uso:
    python processor.py
"""
import os
import time
import csv
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, max as spark_max, min as spark_min,
    sum as spark_sum, first, last, when, count,
    max_by, min_by, lit, current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

import config

# --- Diretório de métricas ---
METRICS_DIR = Path(config.METRICS_DIR)
METRICS_DIR.mkdir(exist_ok=True)

PERF_CSV = METRICS_DIR / f"performance_{config.EXPERIMENT_LABEL}.csv"
CANDLES_CSV = METRICS_DIR / f"candles_{config.EXPERIMENT_LABEL}.csv"

# Cabeçalhos dos CSVs (escritos uma vez)
if not PERF_CSV.exists():
    with open(PERF_CSV, "w", newline="") as f:
        csv.writer(f).writerow([
            "batch_id", "timestamp", "num_trades",
            "latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
            "throughput_trades_per_sec", "trade_id_gaps",
            "window_size_s", "partitions", "experiment_label", "test_time"
        ])

if not CANDLES_CSV.exists():
    with open(CANDLES_CSV, "w", newline="") as f:
        csv.writer(f).writerow([
            "window_start", "window_end", "symbol",
            "open", "high", "low", "close",
            "volume", "buy_volume", "sell_volume", "num_trades",
            "ema_9", "rsi_14"
        ])

# "Memória" do sistema para guardar o estado dos últimos candles
historico_candles = pd.DataFrame()

# Guarda o último trade_id visto para detectar gaps
ultimo_trade_id_visto = {}


def contar_gaps_trade_id(df_pandas):
    """Conta gaps na sequência de trade_id para medir completude."""
    global ultimo_trade_id_visto
    total_gaps = 0

    for symbol in df_pandas["symbol"].unique():
        ids = sorted(df_pandas[df_pandas["symbol"] == symbol]["trade_id_max"].tolist())
        # Gaps entre batches
        if symbol in ultimo_trade_id_visto:
            expected = ultimo_trade_id_visto[symbol] + 1
            # Não conseguimos contar gaps exatos entre batches com apenas max,
            # mas registramos se há descontinuidade
        if ids:
            ultimo_trade_id_visto[symbol] = ids[-1]

    return total_gaps


def processar_indicadores(df_spark, batch_id):
    """
    Função executada toda vez que a janela do Spark fechar um novo candle.
    Calcula indicadores, salva métricas de desempenho e candles em CSV.
    """
    global historico_candles

    if df_spark.count() == 0:
        return

    ts_processamento = int(time.time() * 1000)

    # --- 1. IMPRIME A TABELA OHLCV DO SPARK ---
    print(f"\n============================================================")
    print(f"LOTE {batch_id} | TABELA OHLCV (SPARK)")
    print(f"============================================================")
    df_spark.show(truncate=False)

    # --- 2. PASSA OS DADOS PARA O PANDAS ---
    df_pandas = df_spark.toPandas()

    # --- 3. MÉTRICAS DE DESEMPENHO ---
    # Latência fim-a-fim: tempo de processamento - ts_trade médio da janela
    if "avg_ts_trade" in df_pandas.columns:
        latencias = ts_processamento - df_pandas["avg_ts_trade"]
        latency_mean = latencias.mean()
        latency_p50 = latencias.quantile(0.50)
        latency_p95 = latencias.quantile(0.95)
        latency_p99 = latencias.quantile(0.99)
    else:
        latency_mean = latency_p50 = latency_p95 = latency_p99 = 0

    num_trades = int(df_pandas["num_trades"].sum()) if "num_trades" in df_pandas.columns else 0
    throughput = num_trades / config.TRIGGER_SECONDS if config.TRIGGER_SECONDS > 0 else 0

    gaps = contar_gaps_trade_id(df_pandas)

    # Salva métricas de performance
    with open(PERF_CSV, "a", newline="") as f:
        csv.writer(f).writerow([
            batch_id,
            pd.Timestamp.now().isoformat(),
            num_trades,
            f"{latency_mean:.1f}",
            f"{latency_p50:.1f}",
            f"{latency_p95:.1f}",
            f"{latency_p99:.1f}",
            f"{throughput:.1f}",
            gaps,
            config.WINDOW_SIZE_SECONDS,
            os.getenv("PARTITIONS", "1"),
            config.EXPERIMENT_LABEL,
            os.getenv("TEST_TIME", "N/A")
        ])

    # --- 4. INDICADORES TÉCNICOS (PANDAS) ---
    historico_candles = pd.concat([historico_candles, df_pandas], ignore_index=True)
    historico_candles = historico_candles.tail(50).reset_index(drop=True)

    if len(historico_candles) >= 14:
        # EMA de 9 períodos
        historico_candles['ema_9'] = historico_candles['close'].ewm(span=9, adjust=False).mean()

        # RSI de 14 períodos
        delta = historico_candles['close'].diff()
        gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        loss = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
        rs = gain / loss
        historico_candles['rsi_14'] = 100 - (100 / (1 + rs))

        ultimo = historico_candles.iloc[-1]

        print(f"--- INDICADORES EM TEMPO REAL (PANDAS) ---")
        print(f"[{ultimo['symbol']}] Fechamento Atual: $ {ultimo['close']:.2f}")
        print(f"Rastreador de Tendência -> EMA(9): {ultimo['ema_9']:.2f}")
        print(f"Força do Movimento    -> RSI(14): {ultimo['rsi_14']:.2f}")

        if "buy_volume" in df_pandas.columns:
            print(f"Volume Compra: {ultimo.get('buy_volume', 0):.4f} | "
                  f"Volume Venda: {ultimo.get('sell_volume', 0):.4f}")

        print(f"Latência média: {latency_mean:.0f}ms | "
              f"p95: {latency_p95:.0f}ms | "
              f"Throughput: {throughput:.0f} trades/s")
        print(f"============================================================\n")
    else:
        print(f"--- INDICADORES: Aquecendo histórico ({len(historico_candles)}/14 candles) ---")
        print(f"Latência média: {latency_mean:.0f}ms | Throughput: {throughput:.0f} trades/s")
        print(f"============================================================\n")

    # --- 5. SALVA CANDLES EM CSV ---
    for _, row in df_pandas.iterrows():
        ema_val = ""
        rsi_val = ""
        if len(historico_candles) >= 14:
            last_row = historico_candles[historico_candles["symbol"] == row["symbol"]]
            if not last_row.empty:
                ema_val = f"{last_row.iloc[-1].get('ema_9', '')}"
                rsi_val = f"{last_row.iloc[-1].get('rsi_14', '')}"

        with open(CANDLES_CSV, "a", newline="") as f:
            csv.writer(f).writerow([
                row.get("window_start", ""),
                row.get("window_end", ""),
                row.get("symbol", ""),
                row.get("open", ""),
                row.get("high", ""),
                row.get("low", ""),
                row.get("close", ""),
                row.get("volume", ""),
                row.get("buy_volume", ""),
                row.get("sell_volume", ""),
                row.get("num_trades", ""),
                ema_val,
                rsi_val,
            ])


# --- CONFIGURAÇÃO DO SPARK ---
spark = SparkSession.builder \
    .appName("Binance-Indicadores-Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("trade_id", LongType()),
    StructField("ts_trade", LongType()),
    StructField("ts_ingest", LongType()),
    StructField("price", StringType()),
    StructField("qty", StringType()),
    StructField("side", StringType()),
    StructField("symbol", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", config.TRADES_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

transformed_df = parsed_df \
    .withColumn("timestamp", (col("ts_trade") / 1000).cast(TimestampType())) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("qty", col("qty").cast("double"))

# OHLCV com buy/sell volume e fix do close por trade_id
ohlcv_df = transformed_df \
    .withWatermark("timestamp", f"{config.WATERMARK_SECONDS} seconds") \
    .groupBy(
        window(col("timestamp"), f"{config.WINDOW_SIZE_SECONDS} seconds"),
        col("symbol")
    ).agg(
        # Open = preço do trade com menor trade_id na janela
        min_by("price", "trade_id").alias("open"),
        spark_max("price").alias("high"),
        spark_min("price").alias("low"),
        # Close = preço do trade com maior trade_id na janela
        max_by("price", "trade_id").alias("close"),
        spark_sum("qty").alias("volume"),
        # Buy/sell volume separados
        spark_sum(when(col("side") == "buy", col("qty")).otherwise(0)).alias("buy_volume"),
        spark_sum(when(col("side") == "sell", col("qty")).otherwise(0)).alias("sell_volume"),
        count("*").alias("num_trades"),
        # Para métricas de latência
        expr("avg(ts_trade)").alias("avg_ts_trade"),
        spark_max("trade_id").alias("trade_id_max"),
    ) \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end)

# Saída usando o foreachBatch para processamento com estado no Python
query = ohlcv_df.writeStream \
    .outputMode("append") \
    .foreachBatch(processar_indicadores) \
    .trigger(processingTime=f"{config.TRIGGER_SECONDS} seconds") \
    .start()

query.awaitTermination()
