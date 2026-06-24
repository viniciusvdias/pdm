"""
profiler.py — lógica de perfilamento por chunk (fatia do dataset).

Cada chamada a profile_chunk() é projetada para rodar em um processo separado
(ProcessPoolExecutor). A função é de nível de módulo para ser serializável (picklable) no Windows.

Preserva toda a lógica da classe original DataSetSummary.summarize():
  - filtragem de colunas sensíveis (login, cpf, cnpj)
  - informações de forma (shape)
  - linhas de exemplo (primeira e última)
  - contagem de nulos por coluna
  - colunas categóricas: value_counts para baixa cardinalidade
  - detecção de colunas de data (formato preservado)
  - colunas numéricas: min/max/média/desvio padrão, classificação de cardinalidade
  - valores únicos de colunas objeto (≤20) ou amostra (>20)
"""

import re
from collections import Counter
import pandas as pd

# Padrões de nomes de colunas considerados sensíveis — serão removidos antes do perfilamento
SENSITIVE_PATTERNS = [r"login", r"cpf", r"cnpj"]
_COMPILED_PATTERN = re.compile("|".join(SENSITIVE_PATTERNS), re.IGNORECASE)

MAX_UNIQUE_SET_SIZE = 200  # limite máximo para união de conjuntos em colunas de alta cardinalidade


def _filter_sensitive(df: pd.DataFrame) -> pd.DataFrame:
    # Remove colunas cujos nomes correspondem a padrões sensíveis
    safe = [c for c in df.columns if not _COMPILED_PATTERN.search(c)]
    return df[safe] if safe else df


def _is_date(series: pd.Series):
    """Retorna (is_date: bool, sample_str: str) — verifica se a série parece ser data."""
    clean = series.dropna()
    if clean.empty:
        return False, ""
    s = str(clean.iloc[0])
    has_sep = any(sep in s for sep in ['/', '-', ' '])
    has_digit = any(c.isdigit() for c in s)
    if not (has_sep and has_digit):
        return False, s
    try:
        pd.to_datetime(clean.head(3), format='mixed', dayfirst=False)
        return True, s
    except Exception:
        return False, s


def profile_chunk(args):
    """
    Perfila um chunk (fatia) do dataset.

    args: (df: pd.DataFrame, chunk_id: int)

    Retorna um dicionário com estatísticas parciais deste chunk.
    O dicionário é serializável (sem DataFrames — apenas primitivos Python + Counter/set).
    """
    df, chunk_id = args
    df = _filter_sensitive(df)

    # Retorna estrutura vazia se o chunk não tiver linhas
    if df.empty:
        return {"chunk_id": chunk_id, "n_rows": 0, "columns": [], "dtypes": {},
                "null_counts": {}, "first_row": None, "last_row": None,
                "numeric_stats": {}, "numeric_unique_sets": {},
                "obj_value_counts": {}, "obj_unique_sets": {},
                "obj_high_card_samples": {}, "obj_date_info": {}}

    result = {
        "chunk_id": chunk_id,
        "n_rows": len(df),
        "columns": df.columns.tolist(),
        "dtypes": {c: str(df[c].dtype) for c in df.columns},
        "null_counts": df.isnull().sum().to_dict(),
        "first_row": df.iloc[0].to_dict(),   # primeira linha do chunk
        "last_row": df.iloc[-1].to_dict(),   # última linha do chunk
        "numeric_stats": {},
        "numeric_unique_sets": {},
        "obj_value_counts": {},
        "obj_unique_sets": {},
        "obj_high_card_samples": {},
        "obj_date_info": {},
    }

    # ── Colunas numéricas ──────────────────────────────────────────────────────
    for col in df.select_dtypes(include=["int64", "float64", "int32", "float32"]).columns:
        series = df[col].dropna()
        if series.empty:
            result["numeric_stats"][col] = {
                "min": None, "max": None,
                "sum": 0.0, "sum_sq": 0.0, "count": 0, "n_unique": 0
            }
            continue
        n_unique = int(series.nunique())
        result["numeric_stats"][col] = {
            "min": float(series.min()),
            "max": float(series.max()),
            "sum": float(series.sum()),
            "sum_sq": float((series ** 2).sum()),  # necessário para calcular desvio padrão global
            "count": int(len(series)),
            "n_unique": n_unique,
        }
        # Armazena conjunto exato de valores únicos apenas para colunas de baixa cardinalidade
        if n_unique <= 50:
            result["numeric_unique_sets"][col] = set(series.unique().tolist())

    # ── Colunas do tipo objeto (texto/categórico) ──────────────────────────────
    for col in df.select_dtypes(include="object").columns:
        series = df[col]
        is_date_col, sample_str = _is_date(series)

        # Colunas de data são registradas separadamente e ignoradas aqui
        if is_date_col:
            result["obj_date_info"][col] = sample_str
            continue

        n_unique = int(series.nunique())
        n_rows = len(df)
        threshold = max(10, n_rows // 3)

        # Contagem de valores para colunas com baixa cardinalidade
        if n_unique <= threshold:
            result["obj_value_counts"][col] = Counter(series.dropna().tolist())

        # Conjunto exato de valores únicos para colunas com até 20 valores distintos
        if n_unique <= 20:
            result["obj_unique_sets"][col] = set(series.dropna().unique().tolist())
        elif n_unique < n_rows:
            # Alta cardinalidade — guarda apenas uma amostra para evitar uso excessivo de memória
            sample_vals = series.dropna().unique()[:5].tolist()
            result["obj_high_card_samples"][col] = {
                "n_unique_chunk": n_unique,
                "sample": sample_vals,
            }

    return result