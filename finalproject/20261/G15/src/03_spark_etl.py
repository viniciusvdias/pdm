"""
ETL (PySpark): converte os .jpg soltos em blocos sequenciais .parquet (Pipeline B).

Cada imagem e lida como binario, decodificada UMA vez (Pillow no worker Spark), redimensionada
para 224x224 e gravada como pixels uint8 (224x224x3) serializados. uint8 e SEM PERDA (o pixel
decodificado ja e uint8) e metade do float16. O preprocess do MobileNet fica para o read do 04.
Assim o custo de decode e pago 1x aqui, e nao a cada epoca de treino.

Config via env: RAW_DIR (entrada), PARQUET_DIR (saida), ETL_LIMIT (None=todas), IMGS_PER_FILE.
"""

import os
import time
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType

RAW_DIR       = os.environ.get("RAW_DIR", "/tf/data/raw_jpg")
OUT_DIR       = os.environ.get("PARQUET_DIR", "/tf/data/optimized")
IMG_SIZE      = 224
_limit        = os.environ.get("ETL_LIMIT")
LIMIT         = int(_limit) if _limit not in (None, "") else None   # None = TODAS as imagens
IMGS_PER_FILE = int(os.environ.get("IMGS_PER_FILE", "2000"))        # alvo de imagens por arquivo parquet
SPARK_MEM     = os.environ.get("SPARK_DRIVER_MEMORY", "4g")

spark = (
    SparkSession.builder
    .appName("etl_jpg_to_parquet_uint8")
    .master("local[2]")                                              # 2 vCPU do conteiner
    .config("spark.driver.memory", SPARK_MEM)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "512")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark:", spark.version, "| driver.memory:", spark.conf.get("spark.driver.memory"))
print(f"Entrada: {RAW_DIR}  ->  Saida: {OUT_DIR}")

# Le as imagens como binario: colunas path, modificationTime, length, content (bytes).
df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg")
    .load(RAW_DIR)
)

# label pelo prefixo do nome do arquivo (cat.* -> 0, dog.* -> 1)
df = df.withColumn("fname", F.element_at(F.split(F.col("path"), "/"), -1))
df = df.withColumn("label", F.when(F.lower(F.col("fname")).startswith("cat"), 0).otherwise(1))

if LIMIT is not None:
    df = df.limit(LIMIT)

n_total = df.count()
n_part  = max(2, round(n_total / IMGS_PER_FILE))
print(f"Imagens a processar: {n_total:,}  ->  {n_part} arquivos parquet")

# repartition ANTES do decode: espalha o trabalho pesado nas tasks e ja define o nº de arquivos
# de saida. Embaralha so os bytes JPEG (leves), nao os pixels decodificados.
df = df.repartition(n_part)


# UDF: JPEG bytes -> PIL decode -> resize 224 -> pixels uint8 (0-255) -> bytes.
# Pillow (leve, seguro no worker). Imagem corrompida -> array de zeros (nao derruba a task).
@pandas_udf(BinaryType())
def decode_to_uint8(content: pd.Series) -> pd.Series:
    import io
    import numpy as np
    from PIL import Image

    out = []
    for buf in content:
        try:
            img = Image.open(io.BytesIO(buf)).convert("RGB").resize((IMG_SIZE, IMG_SIZE), Image.BILINEAR)
            out.append(np.asarray(img, dtype=np.uint8).tobytes())    # (224,224,3) em 0-255, sem perda
        except Exception:
            out.append(np.zeros((IMG_SIZE, IMG_SIZE, 3), np.uint8).tobytes())
    return pd.Series(out)


# colunas finais: label + data (pixels uint8 224x224x3 serializados em bytes)
df_out = df.select("label", decode_to_uint8(F.col("content")).alias("data"))

t0 = time.time()
df_out.write.mode("overwrite").parquet(OUT_DIR)     # SEM repartition aqui (ja foi antes do decode)
etl_secs = time.time() - t0
print(f"ETL concluido em {etl_secs:.0f}s  ->  {OUT_DIR}")

# Validacao: le de volta, confere contagem, reshape/dtype/range de 1 registro e tamanho em disco.
back = spark.read.parquet(OUT_DIR)
n_out = back.count()
print(f"Registros no parquet : {n_out:,}")
print(f"Schema               : {back.schema.simpleString()}")

row = back.limit(1).collect()[0]
arr = np.frombuffer(row["data"], dtype=np.uint8).reshape(IMG_SIZE, IMG_SIZE, 3)
print(f"1 registro           : label={row['label']} | shape={arr.shape} | dtype={arr.dtype} "
      f"| range=[{arr.min()}, {arr.max()}]  (esperado 0-255)")

sz = subprocess.run(["du", "-sh", OUT_DIR], capture_output=True, text=True).stdout.split()[0]
n_files = len(list(Path(OUT_DIR).glob("*.parquet")))
print(f"Tamanho em disco     : {sz}  em {n_files} arquivos parquet")
print(f"Bytes por imagem     : {IMG_SIZE*IMG_SIZE*3:,} (uint8)")

spark.stop()
print("Spark encerrado. Parquet uint8 pronto p/ o 04 (benchmark do Pipeline B).")
