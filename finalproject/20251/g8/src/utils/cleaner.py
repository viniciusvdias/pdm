import os
import glob

def replace(base_dir: str, from_str: str, to_str: str, pattern: str = "*", encoding: str = "Windows-1252"):
    """
    Substitui qualquer string em arquivos dentro de um diretório.

    :param base_dir: Caminho para o diretório contendo os arquivos.
    :param from_str: Texto atual a ser substituído.
    :param to_str: Novo texto.
    :param pattern: Padrão de arquivos (default: '*', todos os arquivos).
    :param encoding: Codificação dos arquivos (default: 'utf-8').
    """
    all_files = glob.glob(os.path.join(base_dir, pattern))
    if not all_files:
        print(f"[AVISO] Nenhum arquivo encontrado em: {base_dir} com padrão '{pattern}'")
        return

    for file_path in all_files:
        print(f"[INFO] Substituindo: '{from_str}' → '{to_str}' | Arquivo: {file_path}")
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            content = content.replace(from_str, to_str)
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
        except Exception as e:
            print(f"[ERRO] Falha ao processar {file_path}: {e}")


from pyspark.sql import SparkSession
from utils.dataloader import DataLoader
from utils.workload import Workload

def salvar_csv(
    df=None,
    output_path=None,
    amostra=False,
    frac=0.01,
    seed=42,
    coalesce=True,
    sep=";",
    encoding="UTF-8",
    header=True
):
    """
    Salva um DataFrame como CSV (com ou sem amostragem), com opções de codificação e formato.

    Parâmetros:
    - df (DataFrame): DataFrame Spark a ser salvo.
    - output_path (str): Caminho de saída.
    - amostra (bool): Se True, salva uma amostra do DataFrame.
    - frac (float): Fração da amostra (se `amostra=True`).
    - seed (int): Semente para reprodutibilidade (se `amostra=True`).
    - coalesce (bool): Se True, reduz a 1 partição (para gerar um único arquivo CSV).
    - sep (str): Separador de campos (padrão: ';').
    - encoding (str): Codificação de saída (padrão: 'UTF-8').
    - header (bool): Incluir cabeçalho (padrão: True).

    Retorna:
    - df_resultante (DataFrame): Amostra ou DataFrame original.
    """
    if df is None or output_path is None:
        raise ValueError("Parâmetros 'df' e 'output_path' são obrigatórios.")

    df_resultante = df.sample(withReplacement=False, fraction=frac, seed=seed) if amostra else df

    writer = df_resultante.coalesce(1) if coalesce else df_resultante
    writer.write \
        .mode("overwrite") \
        .option("header", header) \
        .option("sep", sep) \
        .option("encoding", encoding) \
        .csv(output_path)

    print(f"[CSV] salvo em: {output_path}")
    return df_resultante



def salvar_parquet(
    df=None,
    output_path=None,
    amostra=False,
    frac=0.01,
    seed=42,
    coalesce=True
):
    """
    Salva um DataFrame como Parquet (com ou sem amostragem).

    Parâmetros:
    - df (DataFrame): DataFrame Spark a ser salvo.
    - output_path (str): Caminho de saída.
    - amostra (bool): Se True, salva uma amostra do DataFrame.
    - frac (float): Fração da amostra (se `amostra=True`).
    - seed (int): Semente para reprodutibilidade (se `amostra=True`).
    - coalesce (bool): Se True, reduz a 1 partição (útil para testes ou exportação simples).

    Retorna:
    - df_resultante (DataFrame): Amostra ou DataFrame original.
    """
    if df is None or output_path is None:
        raise ValueError("Parâmetros 'df' e 'output_path' são obrigatórios.")

    df_resultante = df.sample(withReplacement=False, fraction=frac, seed=seed) if amostra else df

    writer = df_resultante.coalesce(1) if coalesce else df_resultante
    writer.write.mode("overwrite").parquet(output_path)

    print(f"[PARQUET] salvo em: {output_path}")
    return df_resultante