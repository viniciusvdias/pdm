from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, format_number # Importe 'format_number'
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
import shutil
import os

spark = SparkSession.builder \
    .appName("SalesPrediction") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

df = spark.read.csv("data/arquivo_filtrado.csv", header=True, inferSchema=True)
df = df.withColumn("Sale (Dollars)", col("Sale (Dollars)"))

full_pipeline_model = PipelineModel.load("models/full_prediction_pipeline")

predictions = full_pipeline_model.transform(df)

evaluator = RegressionEvaluator(
    labelCol="Sale (Dollars)",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
print(f"Raiz do Erro Quadrático Médio (RMSE) nas predições: {rmse:.2f}") # Formata o RMSE para 2 casas decimais

resultado = predictions.select(
    col("Store Number"),
    col("Sale (Dollars)").alias("Sale_Original"),
    col("prediction").alias("Sale_Predicted"),
    format_number(abs(col("prediction") - col("Sale (Dollars)")), 1).alias("Absolute_Error")
)

resultado = resultado.na.drop()

output_dir = "results/resultspredictions_temp"
output_file = "results/predictions_final.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

resultado.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

# Mover o CSV gerado para o nome final
csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
if csv_files:
    csv_gerado = csv_files[0]
    csv_gerado_path = os.path.join(output_dir, csv_gerado)
    shutil.move(csv_gerado_path, output_file)
else:
    print(f"Nenhum arquivo CSV encontrado em {output_dir}")

# Remover diretório temporário
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

print(f"Predições salvas em {output_file}")

spark.stop()