"""
aggregator.py — combina resultados parciais de todos os chunks em um resumo final.

Regras de agregação por tipo de estatística:
  - n_rows         : soma total de linhas
  - null_counts    : soma por coluna
  - min/max numérico: mínimo dos mínimos / máximo dos máximos
  - média numérica : média ponderada (soma / contagem)
  - desvio padrão  : recalculado a partir de soma e soma dos quadrados (compatível com Welford)
  - value_counts   : adição de Counter (fusão exata)
  - unique_sets    : união de conjuntos (exata para baixa cardinalidade, aproximada para alta)
  - date_info      : primeira amostra encontrada prevalece
  - first_row      : extraída do chunk_id == 0
  - last_row       : extraída do maior chunk_id
"""

import math
from collections import Counter


def aggregate_chunks(partial_results: list) -> dict:
    """Combina uma lista de resultados parciais dos chunks em um único resultado agregado."""
    if not partial_results:
        return {}

    # Ordena por chunk_id para garantir que first_row e last_row sejam corretos
    partial_results = sorted(partial_results, key=lambda r: r["chunk_id"])
    first = partial_results[0]
    last = partial_results[-1]

    columns = first["columns"]
    dtypes = first["dtypes"]
    total_rows = sum(r["n_rows"] for r in partial_results)

    first_row = first.get("first_row")
    last_row = last.get("last_row")

    # ── Contagem de valores nulos por coluna ───────────────────────────────────
    null_counts = {col: 0 for col in columns}
    for r in partial_results:
        for col, cnt in r["null_counts"].items():
            null_counts[col] = null_counts.get(col, 0) + int(cnt)

    # ── Agregação de colunas numéricas ─────────────────────────────────────────
    # Para cada coluna numérica, calcula min/max, soma, soma dos quadrados,
    # contagem e número aproximado de valores únicos.    
    numeric_agg = {}
    for r in partial_results:
        for col, stats in r["numeric_stats"].items():
            if col not in numeric_agg:
                numeric_agg[col] = {
                    "min": None, "max": None,
                    "sum": 0.0, "sum_sq": 0.0, "count": 0, "n_unique": 0
                }
            a = numeric_agg[col]
            if stats["min"] is not None:
                a["min"] = stats["min"] if a["min"] is None else min(a["min"], stats["min"])
                a["max"] = stats["max"] if a["max"] is None else max(a["max"], stats["max"])
            a["sum"] += stats["sum"]
            a["sum_sq"] += stats["sum_sq"]
            a["count"] += stats["count"]
            # n_unique é aproximado (o máximo visto subestima o valor real entre chunks)
            a["n_unique"] = max(a["n_unique"], stats["n_unique"])

    # Calcula média e desvio padrão global a partir das somas acumuladas
    for col, a in numeric_agg.items():
        n = a["count"]
        if n > 0:
            mean = a["sum"] / n
            variance = (a["sum_sq"] / n) - (mean ** 2)
            a["mean"] = mean
            a["std"] = math.sqrt(max(0.0, variance))
        else:
            a["mean"] = None
            a["std"] = None

    # ── Conjuntos únicos numéricos (exatos para baixa cardinalidade) ───────────
    numeric_unique: dict[str, set] = {}
    for r in partial_results:
        for col, s in r["numeric_unique_sets"].items():
            if col not in numeric_unique:
                numeric_unique[col] = set()
            numeric_unique[col] |= s

    # ── Contagem de valores de colunas objeto (fusão exata via Counter) ────────
    obj_value_counts: dict[str, Counter] = {}
    for r in partial_results:
        for col, counter in r["obj_value_counts"].items():
            if col not in obj_value_counts:
                obj_value_counts[col] = Counter()
            obj_value_counts[col] += counter

    # ── Conjuntos únicos de colunas objeto (união exata para baixa cardinalidade)
    obj_unique_sets: dict[str, set] = {}
    for r in partial_results:
        for col, s in r["obj_unique_sets"].items():
            if col not in obj_unique_sets:
                obj_unique_sets[col] = set()
            obj_unique_sets[col] |= s

    # ── Informações de colunas de data (prevalece a primeira amostra encontrada)
    obj_date_info: dict[str, str] = {}
    for r in partial_results:
        for col, sample in r["obj_date_info"].items():
            if col not in obj_date_info:
                obj_date_info[col] = sample

    # ── Colunas objeto de alta cardinalidade (aproximado por soma dos chunks) ──
    obj_high_card: dict[str, dict] = {}
    for r in partial_results:
        for col, info in r["obj_high_card_samples"].items():
            if col not in obj_high_card:
                obj_high_card[col] = {"n_unique_approx": 0, "sample": info["sample"]}
            obj_high_card[col]["n_unique_approx"] += info["n_unique_chunk"]

    return {
        "total_rows": total_rows,
        "total_cols": len(columns),
        "columns": columns,
        "dtypes": dtypes,
        "first_row": first_row,
        "last_row": last_row,
        "null_counts": null_counts,
        "numeric_agg": numeric_agg,
        "numeric_unique": numeric_unique,
        "obj_value_counts": obj_value_counts,
        "obj_unique_sets": obj_unique_sets,
        "obj_date_info": obj_date_info,
        "obj_high_card": obj_high_card,
    }


def format_summary(agg: dict) -> str:
    """
    Formata o resultado agregado no mesmo formato de texto gerado pelo
    DataSetSummary.summarize() original. Preserva todas as seções do original.
    """
    lines = [
        f"The input dataframe has the following number of rows and columns: "
        f"{agg['total_rows']}x{agg['total_cols']}",
        f"It has the following column names: {agg['columns']}",
        f"And the data types for each column are: {agg['dtypes']}",
        "And these are 2 example rows from the dataframe",
        str(agg.get("first_row")),
        str(agg.get("last_row")),
    ]
    summary = "\n".join(lines) + "\n"

    # ── Colunas categóricas: contagem de valores ───────────────────────────────
    for col, counter in agg["obj_value_counts"].items():
        n_unique = len(counter)
        n_rows = agg["total_rows"]
        threshold = max(10, n_rows // 3)
        if n_unique < n_rows and n_unique <= threshold:
            counts_str = "\n".join(
                f"{k}    {v}"
                for k, v in sorted(counter.items(), key=lambda x: -x[1])
            )
            summary += (
                f"\nColumn '{col}' has {n_unique} unique values out of {n_rows} rows "
                f"(use this column for groupby/aggregation if necessary — it has repeated categories):\n"
                f"{counts_str}\n"
            )

    # ── Colunas de data ────────────────────────────────────────────────────────
    for col, sample in agg["obj_date_info"].items():
        summary += (
            f"\nColumn '{col}' appears to be a DATE column. "
            f"EXACT format in CSV: '{sample}' — preserve this format when plotting, do NOT reformat.\n"
        )

    # ── Colunas numéricas de baixa cardinalidade (≤50 valores únicos) ──────────
    for col in agg["numeric_unique"]:
        n_unique = len(agg["numeric_unique"][col])
        summary += (
            f"\nColumn '{col}' has {n_unique} unique numeric values "
            f"— suitable for groupby/scatter aggregation.\n"
        )

    # ── Estatísticas de colunas numéricas (amplitude e classificação) ──────────
    for col, stats in agg["numeric_agg"].items():
        col_min = stats.get("min")
        col_max = stats.get("max")
        is_low_card = col in agg["numeric_unique"]

        if is_low_card:
            n_unique = len(agg["numeric_unique"][col])
            summary += (
                f"\nColumn '{col}': {n_unique} unique values, range [{col_min} to {col_max}]"
                f" — use integer ticks if plotting counts.\n"
            )
        else:
            if col_min is not None and col_max is not None:
                summary += (
                    f"\nColumn '{col}': continuous numeric, range [{col_min:.2f} to {col_max:.2f}]"
                    f" — do NOT force integer ticks.\n"
                )

    # ── Valores únicos de colunas objeto com até 20 valores distintos ──────────
    for col, unique_set in agg["obj_unique_sets"].items():
        if len(unique_set) <= 20:
            unique_vals = sorted(list(unique_set))
            summary += f"\nColumn '{col}' unique values: {unique_vals}\n"

    # ── Amostra de colunas objeto de alta cardinalidade ────────────────────────
    for col, info in agg["obj_high_card"].items():
        n_unique = info["n_unique_approx"]
        sample = info["sample"]
        summary += (
            f"\nColumn '{col}' has ~{n_unique} unique values. Sample: {sample}\n"
            f"WARNING: category names must match CSV exactly — do NOT truncate or modify them.\n"
        )

    return summary
