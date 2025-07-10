import os
import requests
import zipfile
import time
import logging
import glob
import subprocess
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DoubleType
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EnemPipeline")

# Anotar tempo total
start_total = time.time()

# Configuração geral
YEARS = [2020, 2021, 2023]
HDFS_URI = "hdfs://namenode:8020"
LOCAL_PARQUET_BASE = "/data/enem_clean"
LOCAL_RESULTS_BASE = "/data/enem_results"

# Verificar se deve usar dados locais
USE_LOCAL_DATA = os.getenv('USE_LOCAL_DATA', 'false').lower() == 'true'

if USE_LOCAL_DATA:
    logger.info(" Modo LOCAL ativado - usando dados de amostra")
else:
    logger.info("Modo COMPLETO ativado - baixando dados reais do ENEM")

def get_spark_worker_metrics():
    """
    Coleta métricas dos workers Spark via /json (modo compatível com campos em lowercase)
    - CPU total e por worker
    - RAM média
    - Threads estimadas com base nos executores Spark
    """
    try:
        response = requests.get("http://spark-master:8080/json", timeout=5)
        data = response.json()

        workers = data.get("workers", [])
        num_workers = len(workers)
        if num_workers == 0:
            return 0, 0, 0, 0, 0

        total_cores = sum(w.get("cores", 0) for w in workers)
        used_cores = sum(w.get("coresused", 0) for w in workers)
        total_mem = sum(w.get("memoryused", 0) for w in workers)  # em MB já

        avg_mem = total_mem / num_workers
        avg_cpu_total = (used_cores / total_cores * 100) if total_cores else 0
        avg_cpu_per_worker = avg_cpu_total / num_workers if num_workers else 0

        # Threads estimadas: número de executores (sem o driver)
        executor_status = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
        total_threads = executor_status.keySet().size() - 1

        return num_workers, round(avg_mem, 2), round(avg_cpu_total, 2), round(avg_cpu_per_worker, 2), total_threads

    except Exception as e:
        logger.warning(f"⚠️ Falha ao coletar métricas do Spark Master: {e}")
        return 0, 0, 0, 0, 0
    
    
def get_executor_threads_estimate():
    try:
        response = requests.get("http://spark-master:8080/json", timeout=5)
        data = response.json()
        active_apps = data.get("activeapps", [])
        if not active_apps:
            return 0

        # Obter quantos cores a app está usando
        cores_used = active_apps[0].get("cores", 0)
        workers = data.get("workers", [])
        num_workers = len(workers)

        return cores_used, cores_used / num_workers if num_workers else 0

    except Exception as e:
        logger.warning(f"⚠️ Falha ao estimar threads: {e}")
        return 0, 0


def get_metrics():
    end_total = time.time()
    num_workers, mem_avg, cpu_avg_total, cpu_avg_per_worker, avg_threads = get_spark_worker_metrics()
    throughput = round(total_records / (end_total - start_total), 2)

    cores_total, avg_threads = get_executor_threads_estimate()

    logger.info("🔍 Métricas de Desempenho:")
    logger.info(f"📊 Total de registros processados: {total_records}")
    logger.info(f"🧵 Nº de Workers (Spark): {num_workers}")
    logger.info(f"⏱️ Tempo total de execução: {round(end_total - start_total, 2)}s")
    logger.info(f"🚀 Throughput total: {throughput} linhas/s")
    logger.info(f"💻 CPU total utilizada: {cpu_avg_total}%")
    logger.info(f"💻 CPU média por worker: {cpu_avg_per_worker}%")
    logger.info(f"🧠 RAM média por worker: {mem_avg} MB")
    logger.info(f"🧵 Threads (cores) utilizados: {cores_total}")
    logger.info(f"🧵 Threads médias por worker: {avg_threads}")
    
def get_worker_cores():
    try:
        response = requests.get("http://spark-master:8080/json", timeout=5)
        data = response.json()
        workers = data.get("workers", [])
        if not workers:
            return 2  # valor seguro padrão
        total_threads = sum(w.get("cores", 0) for w in workers)
        return total_threads
    except Exception as e:
        logger.warning(f"⚠️ Erro ao obter número de threads: {e}")
        return 2

    
# Função de download + unzip
def download_and_extract(year):
    url = f"https://download.inep.gov.br/microdados/microdados_enem_{year}.zip"
    local_zip = f"enem{year}.zip"
    extract_dir = f"/data/enem_data/{year}"
    csv_pattern = os.path.join(extract_dir, "**", "MICRODADOS_ENEM_*.csv")
    hdfs_csv_path = f"/user/enem/csv_raw/{year}/MICRODADOS_ENEM_{year}.csv"

    os.makedirs(extract_dir, exist_ok=True)
    logger.info(f"🔍 Verificando presença local dos arquivos do ENEM {year}")

    # Etapa 1: procurar CSV local já extraído
    csv_files = glob.glob(csv_pattern, recursive=True)
    logger.info(f"🔍 Procurando CSVs locais com padrão: {csv_pattern}")
    if csv_files:
        local_csv_path = csv_files[0]
        logger.info(f"📂 CSV local encontrado: {local_csv_path}")

    # Etapa 2: se CSV não existir, mas ZIP sim ➝ extrair
    elif os.path.exists(local_zip):
        logger.info(f"📦 ZIP local encontrado: {local_zip}, extraindo...")
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        os.remove(local_zip)

        csv_files = glob.glob(csv_pattern, recursive=True)
        if not csv_files:
            raise FileNotFoundError(f"❌ Nenhum MICRODADOS_ENEM encontrado após extrair {local_zip}")
        local_csv_path = csv_files[0]
        logger.info(f"📂 CSV extraído com sucesso: {local_csv_path}")

    # Etapa 3: se nem CSV nem ZIP ➝ baixar ZIP e extrair
    else:
        logger.info(f"⬇️ Nenhum CSV ou ZIP encontrado. Baixando de {url}")
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            with session.get(url, stream=True, headers=headers, timeout=60) as r:
                r.raise_for_status()
                with open(local_zip, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao baixar {url}: {e}")
            raise

        logger.info(f"📦 Extraindo {local_zip}")
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        os.remove(local_zip)

        csv_files = glob.glob(csv_pattern, recursive=True)
        if not csv_files:
            raise FileNotFoundError(f"❌ Nenhum MICRODADOS_ENEM encontrado após baixar e extrair {year}")
        local_csv_path = csv_files[0]
        logger.info(f"📂 CSV extraído com sucesso: {local_csv_path}")

    # Agora que temos o CSV local, enviaremos para o HDFS
    mkdirs_hierarchy(f"/user/enem/csv_raw/{year}")
    logger.info(f"📤 Enviando CSV para HDFS: {hdfs_csv_path}")
    copy_to_hdfs(local_csv_path, hdfs_csv_path)

    return hdfs_csv_path

def normalizar_notas(df):
    colunas_numericas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT"]
    for coluna in colunas_numericas:
        df = df.withColumn(coluna, regexp_replace(col(coluna), '"', ''))
        df = df.withColumn(coluna, regexp_replace(col(coluna), ',', '.'))
        df = df.withColumn(coluna, col(coluna).cast("double"))
    return df

def limpar_dados(df):
    """Aplica limpeza e normalização aos dados do ENEM."""
    logger.info("🧹 Iniciando limpeza de dados...")

    colunas_utilizadas = [
    "NU_INSCRICAO", "NU_ANO", "SG_UF_PROVA", "TP_ESCOLA", "Q006",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT"
]

    df = df.select(*colunas_utilizadas)

    def limpar_valores(col):
        return F.when(F.col(col).rlike(r"^(99999|\s*|\.)$"), None).otherwise(F.col(col))

    df = df.withColumn("Q006", limpar_valores("Q006"))

    df = df.withColumn("Q006_VALOR", renda_valor(F.col("Q006")))

    df = df.dropna(subset=["NU_NOTA_MT", "Q006_VALOR", "NU_INSCRICAO", "SG_UF_PROVA"])

    df = df.filter((F.col("NU_NOTA_MT") >= 0) & (F.col("NU_NOTA_MT") <= 1000))

    df = df.dropDuplicates(["NU_INSCRICAO", "NU_ANO"])

    df = df.withColumn("REGIAO", uf_regiao(F.col("SG_UF_PROVA")))

    logger.info("✅ Limpeza concluída")
    return df


def mkdirs_hierarchy(path_str):
    """Cria todos os diretórios pai no HDFS usando API Hadoop"""
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

    parts = path_str.strip("/").split("/")
    current_path = ""

    for part in parts:
        current_path += f"/{part}"
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(current_path)

        if not fs.exists(path_obj):
            created = fs.mkdirs(path_obj)
            if created:
                logger.info(f"✅ Criado diretório: {current_path}")
            else:
                logger.error(f"❌ Falha ao criar diretório: {current_path}")
                raise RuntimeError(f"Erro ao criar {current_path}")
        else:
            logger.info(f"📁 Já existe: {current_path}")
            

def copy_to_hdfs(local_path, hdfs_path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    src_path = spark._jvm.org.apache.hadoop.fs.Path(f"file://{local_path}")
    dst_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

    if fs.exists(dst_path):
        logger.warning(f"⚠️ Arquivo já existe em {hdfs_path}, sobrescrevendo.")
        fs.delete(dst_path, True)

    fs.copyFromLocalFile(False, True, src_path, dst_path)
    logger.info(f"✅ Arquivo copiado via API Hadoop: {hdfs_path}")


# Spark Session
spark = SparkSession.builder \
    .appName("ENEM Pipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "3") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()


# Schema enxuto
schema = StructType([
    StructField("NU_INSCRICAO", LongType(), True),
    StructField("NU_ANO", IntegerType(), True),
    StructField("SG_UF_PROVA", StringType(), True),
    StructField("TP_ESCOLA", IntegerType(), True),
    StructField("Q006", StringType(), True),
    StructField("NU_NOTA_CN", DoubleType(), True),
    StructField("NU_NOTA_CH", DoubleType(), True),
    StructField("NU_NOTA_LC", DoubleType(), True),
    StructField("NU_NOTA_MT", DoubleType(), True)
])


# Mapas auxiliares
renda_map = {
    "A": 300, "B": 500, "C": 900, "D": 1300, "E": 1900, "F": 2500, "G": 3200,
    "H": 3900, "I": 4600, "J": 5400, "K": 6300, "L": 7200, "M": 8100,
    "N": 9100, "O": 10200, "P": 11500
}
state_region = {
    "AC":"Norte","AM":"Norte","AP":"Norte","PA":"Norte","RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste","PB":"Nordeste","PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MT":"Centro-Oeste","MS":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","RS":"Sul","SC":"Sul"
}

# UDFs
@F.udf("int")
def renda_valor(code): return renda_map.get(code, 0)

@F.udf("string")
def uf_regiao(uf): return state_region.get(uf, "Indefinido")

def process_local_data(year):
    """
    Processa dados locais pré-carregados no container (modo rápido)
    """
    local_csv_path = f"/data/enem_data/{year}/DADOS/MICRODADOS_ENEM_{year}.csv"
    hdfs_csv_path = f"/user/enem/csv_raw/{year}/MICRODADOS_ENEM_{year}.csv"
    
    logger.info(f"📂 Verificando dados locais para {year}: {local_csv_path}")
    
    if os.path.exists(local_csv_path):
        logger.info(f"✅ Dados locais encontrados para {year}")
        # Enviar para HDFS
        mkdirs_hierarchy(f"/user/enem/csv_raw/{year}")
        logger.info(f"📤 Enviando dados locais para HDFS: {hdfs_csv_path}")
        copy_to_hdfs(local_csv_path, hdfs_csv_path)
        return hdfs_csv_path
    else:
        logger.warning(f"⚠️ Dados locais não encontrados para {year} em {local_csv_path}")
        logger.info("🔄 Voltando para download automático...")
        return download_and_extract(year)

# Acumulador
df_all = None
total_records = 0

threads = get_worker_cores()
num_partitions = threads * 3
logger.info(f"⚙️ Configurando {num_partitions} partitions para paralelismo com base em {threads} threads")

for year in YEARS:
    t0 = time.time()

    # ETAPA 1 - DOWNLOAD E LEITURA
    if USE_LOCAL_DATA:
        logger.info(f"📂 Processando dados locais para {year}...")
        hdfs_csv_path = process_local_data(year)
    else:
        logger.info(f"🌐 Baixando dados reais para {year}...")
        hdfs_csv_path = download_and_extract(year)
    
    logger.info("✅ Metricas Parciais de Download e Extração:")
    get_metrics()
    
    logger.info(f"Lendo CSV via HDFS: {hdfs_csv_path}")
    df = spark.read.csv(
    f"{HDFS_URI}{hdfs_csv_path}",
    sep=";",
    header=True,
    encoding="ISO-8859-1",
    inferSchema=False
)

    df = normalizar_notas(df)
    df = limpar_dados(df)
    
    df.repartition(num_partitions).write.mode("overwrite").option("header", True).csv(f"{HDFS_URI}/user/enem/csv/{year}")

    total_records += df.count()

    # ETAPA 2 - SALVAR PARQUET LOCAL + HDFS
    local_out = f"{LOCAL_PARQUET_BASE}/{year}"
    hdfs_out = f"{HDFS_URI}/user/enem/parquet/{year}"
    df.write.mode("overwrite").parquet(hdfs_out)

    df = df.withColumn("ANO", F.lit(year))
    df_all = df_all.unionByName(df) if df_all else df

    logger.info(f"Tempo total {year}: {round(time.time() - t0, 2)} segundos")
    
    logger.info("✅ Metricas Parciais de Transformação:")
    get_metrics()

# ETAPA 3 - ANÁLISES COMPLEXAS
logger.info("Executando análises...")

# 1. Média por UF/ano
df_uf = df_all.withColumn(
    "MEDIA_GERAL",
    (F.col("NU_NOTA_CN") + F.col("NU_NOTA_CH") + F.col("NU_NOTA_LC") + F.col("NU_NOTA_MT")) / 4
).groupBy("ANO", "SG_UF_PROVA").agg(
    F.avg("MEDIA_GERAL").alias("MEDIA_GERAL_UF")
)


# 2. Correlação renda vs nota por ano (com verificação)
correlacoes = []
for year in YEARS:
    df_y = df_all.filter(F.col("ANO") == year)
    count = df_y.count()
    if count < 2:
        logger.warning(f"⚠️ Dados insuficientes para calcular correlação em {year} (apenas {count} registros)")
        corr = None
    else:
        stats = df_y.selectExpr("stddev(Q006_VALOR) as std_q006", "stddev(NU_NOTA_MT) as std_mt").collect()[0]
        if stats["std_q006"] == 0 or stats["std_mt"] == 0:
            logger.warning(f"⚠️ Desvio padrão zero em {year}, ignorando correlação")
            corr = None
        else:
            try:
                corr = df_y.stat.corr("Q006_VALOR", "NU_NOTA_MT")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao calcular correlação em {year}: {e}")
                corr = None

    correlacoes.append((year, corr))

schema_corr = StructType([
    StructField("ANO", IntegerType(), False),
    StructField("CORR_Q006_MT", DoubleType(), True)
])

df_corr = spark.createDataFrame(correlacoes, schema=schema_corr)

# 3. Média por tipo de escola
df_escola = df_all.groupBy("ANO", "TP_ESCOLA").agg(F.avg("NU_NOTA_MT").alias("MEDIA_MT"))

# 4. Disparidades regionais (média e desvio)
df_regiao = df_all.groupBy("REGIAO").agg(
    F.avg("NU_NOTA_MT").alias("MEDIA_MT"),
    F.stddev("NU_NOTA_MT").alias("STD_MT"),
    F.count("*").alias("NUM_ESTUDANTES")
)

# 5. Faixas de renda
df_faixa = df_all.withColumn("FAIXA_RENDA", F.when(F.col("Q006_VALOR") <= 1000, "Até 1k")
                                            .when(F.col("Q006_VALOR") <= 3000, "1k–3k")
                                            .when(F.col("Q006_VALOR") <= 6000, "3k–6k")
                                            .otherwise("Acima de 6k")) \
                 .groupBy("ANO", "FAIXA_RENDA").agg(F.avg("NU_NOTA_MT").alias("MEDIA_MT"))

# ETAPA 4 - SALVAR RESULTADOS
def salvar(df, nome):
    df.write.mode("overwrite").parquet(f"{HDFS_URI}/user/enem/resultados/{nome}")

salvar(df_uf, "media_por_uf")
salvar(df_corr, "correlacao_renda")
salvar(df_escola, "media_por_escola")
salvar(df_regiao, "desigualdade_regiao")
salvar(df_faixa, "media_por_faixa_renda")


spark.read.parquet(f"{HDFS_URI}/user/enem/resultados/media_por_uf").show()
spark.read.parquet(f"{HDFS_URI}/user/enem/resultados/media_por_escola").show()
spark.read.parquet(f"{HDFS_URI}/user/enem/resultados/correlacao_renda").show()
spark.read.parquet(f"{HDFS_URI}/user/enem/resultados/desigualdade_regiao").show()
spark.read.parquet(f"{HDFS_URI}/user/enem/resultados/media_por_faixa_renda").show()
    
# ETAPA FINAL - MÉTRICAS

logger.info("✅ Pipeline finalizado")
get_metrics()
