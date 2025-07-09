from pyspark.sql import SparkSession, DataFrame
import os
import glob
from typing import List, Optional

class DataLoader:
    def __init__(
        self,
        spark: SparkSession,
        base_dir: str,
        pattern: str = "*.csv",
        header: bool = True,
        infer_schema: bool = True
    ):
        """
        Inicializa o DataLoader com opções para carregar arquivos CSV de um diretório.

        :param base_dir: Caminho base onde os arquivos estão localizados.
        :param pattern: Padrão dos arquivos CSV (ex: "*.csv").
        :param header: Indica se a primeira linha contém os cabeçalhos.
        :param infer_schema: Tenta inferir automaticamente os tipos de dados.
        """
        self.base_dir = base_dir
        self.pattern = pattern
        self.header = header
        self.infer_schema = infer_schema
        self.spark = spark

    def load(self, selected_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Carrega todos os arquivos CSV correspondentes ao padrão definido e os une em um DataFrame.

        :param selected_columns: Lista de colunas desejadas (opcional).
        :return: DataFrame com os dados.
        """
        all_files = glob.glob(os.path.join(self.base_dir, self.pattern))
        if not all_files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado em {self.base_dir} com padrão {self.pattern}")

        df = self.spark.read.option("header", self.header)\
                            .option("inferSchema", self.infer_schema)\
                            .option("sep", ";")\
                            .option("encoding", "Windows-1252")\
                            .csv(all_files)

        if selected_columns:
            df = df.select(*selected_columns)

        return df

    @staticmethod
    def join(
        df1: DataFrame,
        df2: DataFrame,
        on: List[str],
        how: str = "inner",
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Realiza o join entre dois DataFrames com base nas colunas especificadas.

        :param df1: Primeiro DataFrame.
        :param df2: Segundo DataFrame.
        :param on: Lista de colunas usadas para o join.
        :param how: Tipo de join (inner, left, right, outer).
        :param select_columns: Colunas finais desejadas no resultado.
        :return: DataFrame unido.
        """
        joined_df = df1.join(df2, on=on, how=how)
        if select_columns:
            joined_df = joined_df.select(*select_columns)
        return joined_df
