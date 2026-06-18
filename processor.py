from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, max, min, sum, first, last
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import pandas as pd

# "Memória" do sistema para guardar o estado dos últimos candles
historico_candles = pd.DataFrame()

def processar_indicadores(df_spark, batch_id):
    """
    Função executada toda vez que a janela do Spark fechar um novo candle.
    Recebe o DataFrame do Spark, imprime a tabela original, converte para Pandas e calcula EMA/RSI.
    """
    global historico_candles
    
    # Se o lote estiver vazio, ignora
    if df_spark.count() == 0:
        return

    # --- 1. IMPRIME A TABELA OHLC DO SPARK ---
    print(f"\n============================================================")
    print(f"LOTE {batch_id} | TABELA OHLCV (SPARK)")
    print(f"============================================================")
    df_spark.show(truncate=False)

    # --- 2. PASSA OS DADOS PARA O PANDAS ---
    df_pandas = df_spark.toPandas()
    historico_candles = pd.concat([historico_candles, df_pandas], ignore_index=True)

    # Limpa a memória para não estourar a RAM (mantém apenas os últimos 50 minutos)
    historico_candles = historico_candles.tail(50).reset_index(drop=True)

    # --- 3. MATEMÁTICA DOS INDICADORES ---
    if len(historico_candles) >= 14:
        
        # Média Móvel Exponencial (EMA) de 9 períodos
        historico_candles['ema_9'] = historico_candles['close'].ewm(span=9, adjust=False).mean()

        # Índice de Força Relativa (RSI) de 14 períodos
        delta = historico_candles['close'].diff()
        gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        loss = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
        rs = gain / loss
        historico_candles['rsi_14'] = 100 - (100 / (1 + rs))

        # Pega os dados do último candle calculado
        ultimo = historico_candles.iloc[-1]
        
        print(f"--- INDICADORES EM TEMPO REAL (PANDAS) ---")
        print(f"[{ultimo['symbol']}] Fechamento Atual: $ {ultimo['close']:.2f}")
        print(f"Rastreador de Tendência -> EMA(9): {ultimo['ema_9']:.2f}")
        print(f"Força do Movimento    -> RSI(14): {ultimo['rsi_14']:.2f}")
        print(f"============================================================\n")
        
    else:
        # Aviso de aquecimento
        print(f"--- INDICADORES: Aquecendo histórico ({len(historico_candles)}/14 candles) ---")
        print(f"============================================================\n")


# --- CONFIGURAÇÃO DO SPARK (Igual ao que já funcionava) ---
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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

transformed_df = parsed_df \
    .withColumn("timestamp", (col("ts_trade") / 1000).cast(TimestampType())) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("qty", col("qty").cast("double"))

ohlcv_df = transformed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("symbol")
    ).agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("qty").alias("volume")
    )

# Saída usando o foreachBatch para processamento com estado no Python
query = ohlcv_df.writeStream \
    .outputMode("append") \
    .foreachBatch(processar_indicadores) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()