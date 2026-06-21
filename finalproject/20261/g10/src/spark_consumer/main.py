import os
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, expr, window, to_timestamp, from_unixtime
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

# Força o download do modelo no Driver ANTES de iniciar o Spark Streaming.
# Objetivo aqui é apenas popular o cache local do modelo; o objeto NÃO é
# reutilizado nem serializado para os workers (ver _get_classifier abaixo).
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
def clean_text_python(title, comment, parsedcomment, namespace):
    title = title or ""
    comment = comment or ""
    parsedcomment = parsedcomment or ""
    namespace = namespace or 0
    
    if re.match(r"^Q\d+", title) and parsedcomment:
        return re.sub(r"<[^>]*>", "", parsedcomment)
    if namespace in [6, 14]:
        title = re.sub(r"^(File:|Category:|Categoria:)", "", title)
        
    clean_comment = re.sub(r"\[\[(.*\|)?(.*?)\]\]", r"\2", comment)
    clean_comment = re.sub(r"\/\*.*?\*\/", "", clean_comment).strip()
    return f"{title} {clean_comment}".strip()

clean_text_udf = udf(clean_text_python, StringType())

# --- PIPELINE DE CLASSIFICAÇÃO SEMÂNTICA DIRETA ---
# Carregamos a taxonomia no Driver apenas para os metadados leves (nomes/descrições das categorias).
with open(TAXONOMY_PATH, 'r', encoding='utf-8') as f:
    taxonomy_data = json.load(f)

category_names = [cat['name'] for cat in taxonomy_data['categories']]
text_categories = [f"{cat['name']}: {cat['description']}" for cat in taxonomy_data['categories']]

# O modelo NÃO é capturado no closure da UDF (evitamos serializar ~90MB em CADA task).
# Em vez disso, cada processo worker Python carrega o modelo do cache local UMA única vez,
# sob demanda, e o reutiliza nas chamadas seguintes (acesso, não cópia).
_model = None
_category_vectors = None

def _get_classifier():
    global _model, _category_vectors
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME, device='cpu')
        _category_vectors = _model.encode(text_categories, convert_to_tensor=True)
    return _model, _category_vectors

def classify_text(text_to_classify):
    if not text_to_classify or text_to_classify.strip() == "":
        return "Others|0.0"

    model, category_vectors = _get_classifier()

    # Executa a comparação vetorial
    text_vector = model.encode(text_to_classify, convert_to_tensor=True)
    scores = util.cos_sim(text_vector, category_vectors)[0]
    best_idx = torch.argmax(scores).item()

    return f"{category_names[best_idx]}|{float(scores[best_idx].item()):.2f}"

classify_udf = udf(classify_text, StringType())

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

df_cleaned = df_parsed.withColumn("cleaned_text", clean_text_udf(col("title"), col("comment"), col("parsedcomment"), col("namespace")))
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