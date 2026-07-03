from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, timestamp_seconds
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.streaming import StreamingQueryListener
import pyspark.sql.functions as F
import shutil
import os

# Configurações do Ambiente de Ingestão e Persistência
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092" # Usando o IP de Loopback explícito
TOPIC_NAME = "dados-sensores"
STARTING_OFFSETS = "earliest"  # Garante a captura de toda a carga do simulador
CHECKPOINT_DIR = "/tmp/spark_incendios_checkpoint"

# Limpeza automática do checkpoint antigo para garantir testes limpos e isolados
if os.path.exists(CHECKPOINT_DIR):
    shutil.rmtree(CHECKPOINT_DIR)

# Inicialização da Sessão do Spark otimizada para o Apple Silicon local
spark = SparkSession.builder \
    .appName("ProcessamentoIncendiosSensores") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Reduz o excesso de logs internos da JVM para focar na telemetria da banca
spark.sparkContext.setLogLevel("WARN")

class AuditoriaExecutivaBanca(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"🔬 Monitoramento de Ingestão Ativado. ID da Query: {event.id}")
        
    def onQueryProgress(self, event):
        progress = event.progress
        batch_id = progress.get('batchId')
        
        # O Spark executa o código assíncronamente. Só processamos se houver dados no lote
        if progress.get('numInputRows', 0) > 0:
            total_registros = progress.get('numInputRows')
            
            print("\n" + "="*60)
            print(f"📊 MONITORAMENTO EM TEMPO REAL — LOTE EXTRAÍDO: #{batch_id}")
            print("="*60)
            print(f"📬 Volumetria Ingerida do Kafka : {total_registros} mensagens de sensores.")
            print(f"⏱️ Tempo de Resposta da Camada JDBC: {progress.get('durationMs', {}).get('triggerExecution', 0)} ms")
            print("-"*60)

    def onQueryTerminated(self, event):
        pass

# Registra o novo ouvinte executivo na sessão
spark.streams.addListener(AuditoriaExecutivaBanca())

# Definição do Schema com o Timestamp mapeado diretamente como Long (Unix Epoch)
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperatura", DoubleType(), True),
    StructField("umidade", DoubleType(), True),
    StructField("co2", DoubleType(), True),
    StructField("status_ia_borda", StringType(), True)
])

print(f"🔄 Conectando ao cluster Kafka em {KAFKA_BOOTSTRAP_SERVERS}...")
print(f"📥 Assinando tópico '{TOPIC_NAME}' a partir do offset '{STARTING_OFFSETS}'...")

# Leitura do fluxo contínuo do Apache Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .load()

# Extração e Parsing do Payload JSON do Broker
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_val") \
    .select(from_json(col("json_val"), schema).alias("data")) \
    .select("data.*")

# Regra de Negócio Ponderada (Índice de Risco) e Conversão de Época para Timestamp
df_processado = df_parsed.withColumn(
    "indice_risco",
    expr("""
        CASE 
            WHEN status_ia_borda = 'ALERTA_INCENDIO' THEN 100.0
            ELSE (
                (CASE WHEN temperatura > 30 THEN (temperatura - 30) * 4 ELSE 0 END) +
                (CASE WHEN umidade < 40 THEN (40 - umidade) * 1.5 ELSE 0 END) +
                (CASE WHEN co2 > 450 THEN (co2 - 450) * 0.1 ELSE 0 END)
            )
        END
    """)
).withColumn("timestamp", timestamp_seconds(col("timestamp").cast("long")))

# Função interna para gravação em lote no PostgreSQL com exibição tabular para a banca
def gravar_no_postgres(df_batch, batch_id):
    volumetria = df_batch.count()
    if volumetria > 0:
        # Cache estrutural na RAM para evitar reprocessamento nas chamadas de agregação e show()
        df_batch.cache()
        
        # 1. Computa métricas analíticas e palpáveis para exibição imediata
        media_risco = df_batch.agg(F.avg("indice_risco")).collect()[0][0]
        alertas_criticos = df_batch.filter(col("status_ia_borda") == "ALERTA_INCENDIO").count()
        
        print(f"\n⚡ [PROCESSAMENTO LOTE #{batch_id}] — {volumetria} registros computados em paralelo.")
        print(f"   🔥 Média do Índice de Risco Ambiental (IRI): {media_risco:.2f}%")
        print(f"   🚨 Nós em Estado Crítico de Incêndio: {alertas_criticos} dispositivos.")
        print("\n🔎 Amostra dos dados persistidos no PostgreSQL neste micro-batch:")
        
        # Exibe os dados tabulados de forma limpa na tela
        df_batch.select("sensor_id", "temperatura", "umidade", "co2", "status_ia_borda", "indice_risco") \
                .show(5, truncate=False)
        
        # Configuração JDBC com otimizações em massa (Bulk Writing) para tolerar 1000+ nós
        db_url = "jdbc:postgresql://localhost:5432/incendios_db"
        db_properties = {
            "user": "admin",
            "password": "admin_password",
            "driver": "org.postgresql.Driver",
            "batchsize": "1000",
            "rewriteBatchedInserts": "true"
        }
        
        try:
            df_batch.write \
                .mode("append") \
                .jdbc(url=db_url, table="historico_sensores", properties=db_properties)
            print(f"💾 [SUCESSO] Lote #{batch_id} gravado permanentemente na tabela 'historico_sensores'.")
        except Exception as e:
            print(f"❌ [ERRO DE I/O] Falha ao gravar lote #{batch_id} no PostgreSQL: {e}")
            
        # Desaloca o lote atual da memória RAM
        df_batch.unpersist()

# Inicialização e ativação do pipeline de Streaming Estruturado
try:
    query = df_processado.writeStream \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .foreachBatch(gravar_no_postgres) \
        .start()
    print("🚀 Pipeline de Streaming Ativo! Aguardando transmissões do simulador...")
except Exception as e:
    print(f"❌ Erro fatal ao iniciar o consumidor Spark/Kafka: {e}")
    raise

query.awaitTermination()