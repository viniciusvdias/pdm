from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, month, col
import os, shutil, csv

spark = SparkSession.builder.appName("preprocessing").getOrCreate()

input_path = "data/Liquor_Sales_mod.csv"
output_dir = "data/arquivo_temp_filtrado"
output_file = "data/arquivo_filtrado.csv"
log_file = "data/preprocessamento.csv"

colunas_para_manter = [
    "Date", "Store Number", "Vendor Number", "Bottle Volume (ml)", "State Bottle Cost",
    "State Bottle Retail", "Bottles Sold", "Sale (Dollars)", "County Number",
    "Item Number", "Category"
]

def tamanho_legivel(path):
    tamanho = os.path.getsize(path)
    if tamanho < 1024:
        return f"{tamanho} B"
    elif tamanho < 1024**2:
        return f"{tamanho / 1024:.2f} KB"
    elif tamanho < 1024**3:
        return f"{tamanho / (1024**2):.2f} MB"
    else:
        return f"{tamanho / (1024**3):.2f} GB"

# Lê e filtra
df = spark.read.csv(input_path, header=True, inferSchema=True)
df = df.select(*colunas_para_manter)
df = df.withColumn("Date", to_date("Date", "MM/dd/yyyy"))
df = df.withColumn("month", month(col("Date")))

# Estatísticas
total_linhas = df.count()
linhas_com_nulos = df.filter(
    sum([col(c).isNull().cast("int") for c in df.columns]) > 0
).count()
porcentagem_nulos = (linhas_com_nulos / total_linhas) * 100 if total_linhas > 0 else 0

# Drop nulos
df = df.dropna()
linhas_pos_filtragem = df.count()

# Salvar CSV filtrado
df.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")
csv_gerado = [f for f in os.listdir(output_dir) if f.endswith(".csv")][0]
shutil.move(os.path.join(output_dir, csv_gerado), output_file)
shutil.rmtree(output_dir)

tamanho_original = tamanho_legivel(input_path)
tamanho_filtrado = tamanho_legivel(output_file)

with open(log_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Métrica", "Valor"])
    writer.writerow(["Total de linhas", total_linhas])
    writer.writerow(["Linhas com nulos", linhas_com_nulos])
    writer.writerow(["% Linhas com nulos", f"{porcentagem_nulos:.2f}%"])
    writer.writerow(["Linhas após remoção", linhas_pos_filtragem])
    writer.writerow(["Tamanho do arquivo original", tamanho_original])
    writer.writerow(["Tamanho do arquivo filtrado", tamanho_filtrado])

print("Pré-processamento finalizado e informações salvas em preprocessamento.csv.")
