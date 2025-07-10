from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import pyspark.sql.functions as F
import time
import os

for path in ['datalake/raw', 'datalake/trusted', 'datalake/refined', 'datalake/refined_raw', 'datalake/plots']:
    os.makedirs(path, exist_ok=True)

    
metrics = []

def get_dir_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    return total_size / (1024 * 1024)


def remove_unused_cols(df):
    return df.drop(
        'regiao_sigla',
        'revenda',
        'cnpj_revenda',
        'nome_rua',
        'numero_rua',
        'complemento',
        'bairro',
        'cep',
        'unidade_medida',
        'bandeira',
    )


def add_col_semestre(df):
    return df.withColumn(
        "semestre",
        F.concat(
            F.year("data_coleta"),
            F.lit("-"),
            when(F.month("data_coleta") <= 6, F.lit("01")).otherwise(F.lit("02"))
        )
    )


def add_col_mandato(df):
    return df.withColumn(
        "mandato",
        when(F.col("data_coleta").between('2003-01-01', '2006-12-31'), F.lit("Lula-1"))
        .when(F.col("data_coleta").between('2007-01-01', '2010-12-31'), F.lit("Lula-2"))
        .when(F.col("data_coleta").between('2011-01-01', '2014-12-31'), F.lit("Dilma-1"))
        .when(F.col("data_coleta").between('2015-01-01', '2016-08-31'), F.lit("Dilma-2"))
        .when(F.col("data_coleta").between('2016-09-01', '2018-12-31'), F.lit("Temer"))
        .when(F.col("data_coleta").between('2019-01-01', '2022-12-31'), F.lit("Bolsonaro"))
        .otherwise(F.lit(None))
    )


def add_col_lucro(df):
    return df.withColumn("lucro", round(F.col("valor_venda") - F.col("valor_compra"), 4))


def create_dimensional_modeling(df, source='Trusted', path='datalake/refined'):
    dim_endereco_df = df.select('municipio', 'estado_sigla').distinct().orderBy('municipio')
    dim_endereco_df = dim_endereco_df.withColumn('id_endereco', F.monotonically_increasing_id())

    dim_produto_df = spark.createDataFrame([('GASOLINA',), ('ETANOL',), ('DIESEL',)], ['produto'])
    dim_produto_df = dim_produto_df.withColumn('id_produto', F.monotonically_increasing_id())

    dim_tempo_df = (
    df.select('data_coleta', 'semestre', 'mandato')
        .distinct()
        .withColumn('id_tempo', F.monotonically_increasing_id())
        .withColumn('dia_coleta', F.day('data_coleta'))
        .withColumn('mes_coleta', F.month('data_coleta'))
        .withColumn('ano_coleta', F.year('data_coleta'))
    )

    fato_transacao_df = (
        df.join(dim_produto_df, on='produto', how='left')
        .join(dim_tempo_df, on=['data_coleta', 'semestre', 'mandato'], how='left')
        .join(dim_endereco_df, on=['municipio', 'estado_sigla'], how='left')
        .select(
            'id_produto',
            'id_tempo',
            'id_endereco',
            'valor_venda',
            'valor_compra',
            'lucro'
        )
    )
    fato_transacao_df = fato_transacao_df.withColumn('id_transacao', F.monotonically_increasing_id())

    start_time = time.time()

    dim_endereco_df.write.mode('overwrite').parquet(f'{path}/dim_endereco')
    dim_produto_df.write.mode('overwrite').parquet(f'{path}/dim_produto')
    dim_tempo_df.write.mode('overwrite').parquet(f'{path}/dim_tempo')
    fato_transacao_df.write.mode('overwrite').parquet(f'{path}/fato_transacao')

    end_time = time.time()
    metrics.append((f'Chegada de arquivos ({source} - Refined)', (end_time - start_time)))

    dim_produto_df = spark.read.parquet(f'{path}/dim_produto')
    dim_tempo_df = spark.read.parquet(f'{path}/dim_tempo')
    dim_endereco_df = spark.read.parquet(f'{path}/dim_endereco')
    fato_transacao_df = spark.read.parquet(f'{path}/fato_transacao')

    return (
        fato_transacao_df
        .join(dim_produto_df, on='id_produto', how='left')
        .join(dim_tempo_df, on='id_tempo', how='left')
        .join(dim_endereco_df, on='id_endereco', how='left')
    )


import unicodedata

def remover_acentos(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')

import matplotlib.pyplot as plt
import pandas as pd

def plot_media_vendas_por_semestre(df, agg_col, titulo, produtos=None, figsize=(10 , 6)):
    if produtos:
        df = df.filter(F.col("produto").isin(produtos))


    df_media = df.groupBy('semestre', 'produto').agg(
        F.avg(agg_col).alias('media')
    )

    df_media = df_media.orderBy('semestre')

    df_pandas = df_media.toPandas()

    plt.figure(figsize=figsize)

    for produto in df_pandas['produto'].unique():
        df_produto = df_pandas[df_pandas['produto'] == produto]
        plt.plot(df_produto['semestre'], df_produto['media'], label=produto, marker='o')


    plt.title(titulo)
    plt.xlabel('Semestre')
    plt.ylabel('Média (R$)')
    plt.xticks(rotation=45)
    plt.legend(title="Produto", loc='upper left')
    
    nome_arquivo = remover_acentos(titulo).lower().replace(" ", "_")

    output_path = f'datalake/plots/{nome_arquivo}.png'

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, bbox_inches='tight')


from pyspark.sql.functions import round

def avg_fuel_prices_per_mandato(df, dataframe):
    print(f'Média do valor de venda da gasolina por mandato presidencial ({dataframe}):')
    df_produtos_validos = df.filter(F.col("produto").isin("GASOLINA", "DIESEL", "ETANOL"))
    df_produtos_validos.groupBy("mandato", "produto") \
          .agg(round(F.avg("valor_venda"), 2).alias("media_valor_venda")) \
          .orderBy('media_valor_venda', ascending=False) \
          .show(20)


spark = SparkSession.builder \
    .appName("MeuApp") \
    .master("local[*]") \
    .getOrCreate()

df_raw = spark.read.csv('datalake/raw/',sep=';',header=True)

df_raw = df_raw.toDF('regiao_sigla',
    'estado_sigla',
    'municipio',
    'revenda',
    'cnpj_revenda',
    'nome_rua',
    'numero_rua',
    'complemento',
    'bairro',
    'cep',
    'produto',
    'data_coleta',
    'valor_venda',
    'valor_compra',
    'unidade_medida',
    'bandeira')

df_raw = df_raw.withColumn('valor_venda', F.regexp_replace('valor_venda', ',', '.').cast('double'))
df_raw = df_raw.withColumn('valor_compra', F.regexp_replace('valor_compra', ',', '.').cast('double'))
df_raw = df_raw.withColumn('data_coleta', F.to_date(F.col('data_coleta'), 'dd/MM/yyyy'))

# forcando o processamento batch com o show
df_raw.show()

print("Esquema do DataFrame df_raw:")
df_raw.printSchema()

start_time = time.time()
df_raw.write.mode('overwrite').parquet('datalake/trusted/')
end_time = time.time()

df_trusted = spark.read.parquet('datalake/trusted/')

df_raw = remove_unused_cols(df_raw)
df_raw = add_col_semestre(df_raw)
df_raw = add_col_mandato(df_raw)
df_raw = add_col_lucro(df_raw)

df_trusted = remove_unused_cols(df_trusted)
df_trusted = add_col_semestre(df_trusted)
df_trusted = add_col_mandato(df_trusted)
df_trusted = add_col_lucro(df_trusted)

df_refined_raw = create_dimensional_modeling(df_raw, 'Raw', 'datalake/refined_raw')
df_refined = create_dimensional_modeling(df_trusted)

# Printando os graficos para os quatro dataframes
start_time = time.time()
plot_media_vendas_por_semestre(df_raw, 'valor_venda', 'Média de Valor de Vendas por Semestre e Tipo de Combustível (Raw)', produtos=['GASOLINA','ETANOL','DIESEL'])
plot_media_vendas_por_semestre(df_raw, 'lucro', 'Média de Lucro por Semestre e Tipo de Combustível (Raw)', produtos=['GASOLINA','ETANOL','DIESEL'])
avg_fuel_prices_per_mandato(df_raw, 'Raw')
end_time = time.time()
metrics.append(('Tempo de consulta (Raw)', (end_time - start_time)))

start_time = time.time()
plot_media_vendas_por_semestre(df_trusted, 'valor_venda', 'Média de Valor de Vendas por Semestre e Tipo de Combustível (Trusted)', produtos=['GASOLINA','ETANOL','DIESEL'])
plot_media_vendas_por_semestre(df_trusted, 'lucro', 'Média de Lucro por Semestre e Tipo de Combustível (Trusted)', produtos=['GASOLINA','ETANOL','DIESEL'])
avg_fuel_prices_per_mandato(df_trusted, 'Trusted')
end_time = time.time()
metrics.append(('Tempo de consulta (Trusted)', (end_time - start_time)))

start_time = time.time()
plot_media_vendas_por_semestre(df_refined, 'valor_venda', 'Média de Valor de Vendas por Semestre e Tipo de Combustível (Refined)', produtos=['GASOLINA','ETANOL','DIESEL'])
plot_media_vendas_por_semestre(df_refined, 'lucro', 'Média de Lucro por Semestre e Tipo de Combustível (Refined)', produtos=['GASOLINA','ETANOL','DIESEL'])
avg_fuel_prices_per_mandato(df_refined, 'Refined')
end_time = time.time()
metrics.append(('Tempo de consulta (Refined)', (end_time - start_time)))

start_time = time.time()
plot_media_vendas_por_semestre(df_refined_raw, 'valor_venda', 'Média de Valor de Vendas por Semestre e Tipo de Combustível (Refined)', produtos=['GASOLINA','ETANOL','DIESEL'])
plot_media_vendas_por_semestre(df_refined_raw, 'lucro', 'Média de Lucro por Semestre e Tipo de Combustível (Refined)', produtos=['GASOLINA','ETANOL','DIESEL'])
avg_fuel_prices_per_mandato(df_refined_raw, 'Refined')
end_time = time.time()
metrics.append(('Tempo de consulta (Raw - Refined)', (end_time - start_time)))

# printando os resultados coletados
for metric in metrics:
    minutes, seconds = divmod((metric[1]), 60)
    print(f'{metric[0]}: {int(minutes)} minutos e {int(seconds)} segundos')

print(f"Tamanho do diretório (Raw): {(get_dir_size('datalake/raw')):.2f} MB")
print(f"Tamanho do diretório (Trusted): {(get_dir_size('datalake/trusted')):.2f} MB")
print(f"Tamanho do diretório (Refined): {(get_dir_size('datalake/refined')):.2f} MB")
print(f"Tamanho do diretório (Raw - Refined): {(get_dir_size('datalake/refined_raw')):.2f} MB")