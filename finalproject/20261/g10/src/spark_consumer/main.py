import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, pandas_udf, expr, window, to_timestamp, from_unixtime,
    coalesce, lit, when, regexp_replace, trim, concat_ws,
)
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType

# --- CONFIGURAÇÕES DE AMBIENTE ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'wikimediaRecentchange')
WINDOW_DURATION = os.environ.get('WINDOW_DURATION', '5 minutes')
WINDOW_SLIDE_DURATION = os.environ.get('WINDOW_SLIDE_DURATION', WINDOW_DURATION)

TAXONOMY_PATH = '/misc/topics.json'
MODEL_NAME = 'all-MiniLM-L6-v2'

print("Initializing and testing the NLP environment...", flush=True)
try:
    from sentence_transformers import SentenceTransformer, util
    import torch
except ImportError:
    raise RuntimeError(
        "NLP dependencies are missing from the spark-consumer image. "
        "Rebuild the service to install sentence-transformers and torch."
    )

# Seleciona GPU automaticamente quando disponível (ex.: RTX 50xx / Blackwell); caso contrário, CPU.
DEVICE = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f"Torch device selected for inference: {DEVICE}", flush=True)

# Força o download do modelo no Driver ANTES de iniciar o Spark Streaming.
# Objetivo aqui é apenas popular o cache local do modelo; o objeto NÃO é
# reutilizado nem serializado para os workers (ver _get_classifier abaixo).
# Warmup em CPU para não alocar memória de GPU no processo Driver.
print("Downloading/loading the MiniLM model (ensuring cache)...", flush=True)
SentenceTransformer(MODEL_NAME, device='cpu')
print("Model loaded successfully!", flush=True)

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

# --- LIMPEZA (WORKLOAD-2) ---
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

# --- PIPELINE DE CLASSIFICAÇÃO SEMÂNTICA DIRETA ---
# Carregamos a taxonomia no Driver apenas para os metadados leves (nomes/descrições das categorias).
with open(TAXONOMY_PATH, 'r', encoding='utf-8') as f:
    taxonomy_data = json.load(f)

category_names = [cat['name'] for cat in taxonomy_data['categories']]
text_categories = [f"{cat['name']}: {cat['description']}" for cat in taxonomy_data['categories']]


_model = None
_category_vectors = None

def _get_classifier():
    global _model, _category_vectors
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME, device=DEVICE)
        _category_vectors = _model.encode(text_categories, convert_to_tensor=True)
    return _model, _category_vectors

ENCODE_BATCH_SIZE = int(os.environ.get('ENCODE_BATCH_SIZE', '64'))

# Pandas UDF (vetorizada via Arrow): recebe a coluna inteira do micro-batch como uma
# pandas.Series e faz UMA única chamada de inferência em lote (model.encode em toda a
# lista).
@pandas_udf(StringType())
def classify_udf(texts: pd.Series) -> pd.Series:
    model, category_vectors = _get_classifier()

    s = texts.fillna("").astype(str)
    nonempty = s.str.strip() != ""

    # Linhas vazias não vão para o modelo
    results = pd.Series(["Others|0.0"] * len(s), index=s.index)

    if nonempty.any():
        batch = s[nonempty].tolist()
        embeddings = model.encode(batch, convert_to_tensor=True, batch_size=ENCODE_BATCH_SIZE)
        scores = util.cos_sim(embeddings, category_vectors)  # [N, num_categorias]
        best_idx = torch.argmax(scores, dim=1).tolist()
        best_val = torch.max(scores, dim=1).values.tolist()
        results[nonempty] = [f"{category_names[i]}|{v:.2f}" for i, v in zip(best_idx, best_val)]

    return results

# --- STREAMING ---
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
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
    expr("concat(date_format(time_window.start, 'HH:mm'), '-', date_format(time_window.end, 'HH:mm'))").alias("janela"),
    col("category").alias("categoria"),
    col("count").alias("quantidade")
)

# Exibe o resultado agregado por janela no Console
query = df_result \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "200") \
    .start()

query.awaitTermination()