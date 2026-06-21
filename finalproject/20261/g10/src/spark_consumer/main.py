import os
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, expr
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# --- CONFIGURAÇÕES DE AMBIENTE ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'wikimediaRecentchange')
TAXONOMY_PATH = '/misc/topics.json'

print("🧠 Inicializando e testando ambiente de NLP...", flush=True)
try:
    from sentence_transformers import SentenceTransformer, util
    import torch
except ImportError:
    raise RuntimeError(
        "Dependências de NLP ausentes da imagem do spark-consumer. "
        "Refaça o build do serviço para instalar sentence-transformers e torch."
    )

# Força o download do modelo no Driver ANTES de iniciar o Spark Streaming
print("📥 Baixando/Carregando modelo MiniLM (Garantindo Cache)...", flush=True)
dummy_model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
print("✅ Modelo carregado com sucesso!", flush=True)

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
    StructField("parsedcomment", StringType(), True)
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
# Como rodamos em CPU local simples, a UDF padrão com modelo global evita o overhead de RDDs no streaming básico
with open(TAXONOMY_PATH, 'r', encoding='utf-8') as f:
    taxonomy_data = json.load(f)

category_names = [cat['name'] for cat in taxonomy_data['categories']]
text_categories = [f"{cat['name']}: {cat['description']}" for cat in taxonomy_data['categories']]
category_vectors = dummy_model.encode(text_categories, convert_to_tensor=True)

def classify_text(text_to_classify):
    if not text_to_classify or text_to_classify.strip() == "":
        return "Others|0.0"
    
    # Executa a comparação vetorial
    text_vector = dummy_model.encode(text_to_classify, convert_to_tensor=True)
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

# Aplica a IA e quebra o resultado em Categoria e Confiança
df_inference = df_cleaned.withColumn("inference_raw", classify_udf(col("cleaned_text"))) \
    .withColumn("category", expr("split(inference_raw, '\\\\|')[0]")) \
    .withColumn("confidence", expr("split(inference_raw, '\\\\|')[1]"))

# Exibe o resultado direto no Console
query = df_inference.select("title", "cleaned_text", "category", "confidence") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()