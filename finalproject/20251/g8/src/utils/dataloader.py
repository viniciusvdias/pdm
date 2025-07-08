from pyspark.sql import SparkSession
import os
import glob

class DataLoader:
    def __init__(self, base_dir, pattern="*.csv", header=True, infer_schema=True):
        self.base_dir = base_dir
        self.pattern = pattern
        self.header = header
        self.infer_schema = infer_schema
        self.spark = SparkSession.builder.getOrCreate()

    def load(self):
        """
        Lê todos os arquivos CSV do diretório base que correspondem ao padrão
        e os concatena em um único DataFrame.
        """
        all_files = glob.glob(os.path.join(self.base_dir, self.pattern))
        if not all_files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado em {self.base_dir} com padrão {self.pattern}")

        df = self.spark.read.option("header", self.header)\
                             .option("inferSchema", self.infer_schema)\
                             .option("sep", ";")\
                             .csv(all_files)
        return df
