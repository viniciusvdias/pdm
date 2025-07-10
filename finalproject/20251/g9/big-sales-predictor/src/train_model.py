from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline # Importe Pipeline

spark = SparkSession.builder \
    .appName("SalesTraining") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

df = spark.read.csv("data/arquivo_filtrado.csv", header=True, inferSchema=True)

# Indexar categorias (1 índice por categoria)
categorical_cols = ["Store Number", "Vendor Number", "County Number", "Item Number", "Category"]
indexed_cols = [c + "_idx" for c in categorical_cols]

indexers = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep") for c in categorical_cols]

# Colunas numéricas
numeric_cols = ["Bottle Volume (ml)", "State Bottle Cost", "State Bottle Retail", "Bottles Sold", "month"]

# Montar vetor com categorias indexadas + numéricas
assembler = VectorAssembler(
    inputCols=indexed_cols + numeric_cols,
    outputCol="features"
)

df = df.withColumn("label", col("Sale (Dollars)")).na.drop()

train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="label", regParam=0.1)

pipeline = Pipeline(stages=indexers + [assembler, lr])

model = pipeline.fit(train_data) # O pipeline aprende as transformações e treina o modelo

# Salvar o PipelineModel completo
model.save("models/full_prediction_pipeline") 

spark.stop()