from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, max, min, sum, first, last
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Função para imprimir apenas se a tabela tiver dados
def imprimir_apenas_cheios(df_batch, batch_id):
    if df_batch.count() > 0:
        print(f"--- Lote de Fechamento de Candle: {batch_id} ---")
        df_batch.show(truncate=False)


# Iniciar a sessão do Spark com a versão exata do seu pacote (4.1.2)
spark = SparkSession.builder \
    .appName("Binance-OHLC-Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Lendo os valores numéricos como String primeiro para evitar erro de parse do JSON
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

# 2. Convertendo os tipos: tempo para Timestamp e valores para Double
transformed_df = parsed_df \
    .withColumn("timestamp", (col("ts_trade") / 1000).cast(TimestampType())) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("qty", col("qty").cast("double"))

# 3. Janelamento e Agregação (OHLCV)
ohlcv_df = transformed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ).agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("qty").alias("volume")
    )

# 4. Modo APPEND: Exibe a tabela apenas quando o candle estiver fechado e imutável
query = ohlcv_df.writeStream \
.outputMode("append") \
    .foreachBatch(imprimir_apenas_cheios) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()