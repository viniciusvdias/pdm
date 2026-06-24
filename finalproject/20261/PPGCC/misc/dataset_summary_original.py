import pandas as pd
import re

class DataSetSummary:
    # Padrões que indicam colunas com dados sensíveis — serão ignoradas no resumo
    # r"id", r"classid"
    sensitive_patterns = [
        r"login", r"cpf", r"cnpj"
    ]

    def __init__(self, path: str):
        """Inicializa a classe com o caminho do arquivo CSV."""
        self.path = path
        self.df = None

    def _ignore_sensitive_patterns(self, df):
        # Combina todos os padrões em um único regex (sem distinção de maiúsculas/minúsculas)
        combined_pattern = re.compile("|".join(self.sensitive_patterns), re.IGNORECASE)

        safe_columns = [col for col in df.columns if not combined_pattern.search(col)]
        if not safe_columns:
            return df  # fallback: retorna df original se todas as colunas forem "sensíveis"
        return df[safe_columns]

    def summarize(self):
        # Lê o CSV e remove colunas sensíveis antes de processar
        self.df = pd.read_csv(self.path)
        self.df = self._ignore_sensitive_patterns(self.df)

        colls_names = self.df.columns.values
        lines_num, colls_num = self.df.shape
        coll_types = self.df.dtypes

        # Obtém a primeira e a última linha do dataframe como exemplos
        data_examples = pd.concat([self.df.iloc[[0]], self.df.iloc[[-1]]])

        summary = "The input dataframe has the following number of rows and columns: " + str(lines_num) + "x" + str(colls_num) + "\n" \
            "It has the following column names: " + str(colls_names) + "\n" \
            "And the data types for each column are: " + str(coll_types)+ "\n" \
            "And these are 2 example rows from the dataframe \n" + str(data_examples)+ "\n" \

        # Processa colunas categóricas (tipo objeto) de baixa cardinalidade
        for col in self.df.select_dtypes(include='object').columns:
            # Pula colunas de data — serão tratadas no bloco seguinte
            sample = self.df[col].dropna().iloc[0] if not self.df[col].dropna().empty else ""
            is_date = any(sep in str(sample) for sep in ['/', '-']) and any(c.isdigit() for c in str(sample))
            if is_date:
                continue

            n_unique = self.df[col].nunique()
            n_rows = len(self.df)

            # Limiar dinâmico: colunas com poucos valores únicos são consideradas categóricas
            threshold = max(10, n_rows // 3)

            if n_unique < n_rows and n_unique <= threshold:
                counts = self.df[col].value_counts(sort=False)
                summary += f"\nColumn '{col}' has {n_unique} unique values out of {n_rows} rows "
                summary += f"(use this column for groupby/aggregation if necessary — it has repeated categories):\n"
                summary += f"{counts.to_string()}\n"

        # Detecta e descreve colunas de data (preserva o formato exato do CSV)
        for col in self.df.select_dtypes(include='object').columns:
            sample = self.df[col].dropna().iloc[0] if not self.df[col].dropna().empty else ""
            if any(sep in str(sample) for sep in ['/', '-', ' ']) and any(c.isdigit() for c in str(sample)):
                try:
                    pd.to_datetime(self.df[col].dropna().head(3), format='mixed', dayfirst=False)
                    summary += f"\nColumn '{col}' appears to be a DATE column. "
                    summary += f"EXACT format in CSV: '{sample}' — preserve this format when plotting, do NOT reformat.\n"
                except:
                    pass

        # Colunas numéricas com poucos valores únicos são adequadas para agrupamento
        for col in self.df.select_dtypes(include=['int64', 'float64']).columns:
            n_unique = self.df[col].nunique()
            n_rows = len(self.df)
            if n_unique <= 50:
                summary += f"\nColumn '{col}' has {n_unique} unique numeric values — suitable for groupby/scatter aggregation.\n"

        # Estatísticas de amplitude para colunas numéricas (min, max e classificação)
        for col in self.df.select_dtypes(include=['int64', 'float64']).columns:
            n_unique = self.df[col].nunique()
            n_rows = len(self.df)
            col_min = self.df[col].min()
            col_max = self.df[col].max()

            # Poucos valores únicos → provavelmente categórica ou contagem (usa ticks inteiros)
            if n_unique <= 50:
                summary += f"\nColumn '{col}': {n_unique} unique values, range [{col_min} to {col_max}]"
                summary += f" — use integer ticks if plotting counts.\n"
            else:
                # Muitos valores únicos → coluna contínua (não forçar ticks inteiros)
                summary += f"\nColumn '{col}': continuous numeric, range [{col_min:.2f} to {col_max:.2f}]"
                summary += f" — do NOT force integer ticks.\n"

        # Para colunas categóricas com até 20 valores, lista todos em ordem
        for col in self.df.select_dtypes(include='object').columns:
            sample = self.df[col].dropna().iloc[0] if not self.df[col].dropna().empty else ""
            is_date = any(sep in str(sample) for sep in ['/', '-']) and any(c.isdigit() for c in str(sample))
            if is_date:
                continue

            n_unique = self.df[col].nunique()
            n_rows = len(self.df)
            if n_unique <= 20 and n_unique > 0:
                unique_vals = sorted(self.df[col].dropna().unique().tolist())
                summary += f"\nColumn '{col}' unique values: {unique_vals}\n"

        # Para colunas com muitos valores únicos, mostra amostra dos primeiros valores
        for col in self.df.select_dtypes(include='object').columns:
            sample = self.df[col].dropna().iloc[0] if not self.df[col].dropna().empty else ""
            is_date = any(sep in str(sample) for sep in ['/', '-']) and any(c.isdigit() for c in str(sample))
            if is_date:
                continue

            n_unique = self.df[col].nunique()
            n_rows = len(self.df)

            # Alta cardinalidade — exibe apenas amostra para não sobrecarregar o resumo
            if n_unique > 20 and n_unique < n_rows:
                sample_vals = self.df[col].dropna().unique()[:5].tolist()
                summary += f"\nColumn '{col}' has {n_unique} unique values. Sample: {sample_vals}\n"
                summary += f"WARNING: category names must match CSV exactly — do NOT truncate or modify them.\n"

        return summary

    
"""if __name__ == "__main__":
    path = "visEval_dataset/databases/activity_1/Student.csv"
    dataset_summary = DataSetSummary(path)
    #dataset_summary.csv_read(path)
    print( dataset_summary.summarize())"""