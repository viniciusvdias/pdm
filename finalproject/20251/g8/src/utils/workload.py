from time import perf_counter
from pyspark.sql import DataFrame
from pyspark import SparkContext

class Workload:
    def __init__(self, name, description, execute_fn):
        """
        Representa uma carga de trabalho (workload) associada aos dados carregados.

        :param name: Nome da carga de trabalho.
        :param description: Descrição do objetivo da análise.
        :param execute_fn: Função que recebe um DataFrame e executa a análise.
        """
        self.name = name
        self.description = description
        self.execute_fn = execute_fn
    
    def __str__(self):
        return f"{self.description} ({self.name})"

    def run(self, *args, **kwargs):
        print(str(self))
        start = perf_counter()
    
        result = self.execute_fn(*args, **kwargs)
    
        elapsed_time = perf_counter() - start
        print(f"[METRIC] Tempo de execução ({self.name}): {elapsed_time:.2f} segundos")
    
        return result
