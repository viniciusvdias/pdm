import time

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

    def run(self, df):
        print(str(self))
        start_time = time.time()

        result = self.execute_fn(df)

        elapsed_time = time.time() - start_time
        print(f"[INFO] Tempo de execução da workload '{self.name}': {elapsed_time:.2f} segundos")

        return result
