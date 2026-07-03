"""Reusable Markdown sections for benchmark reports."""

LPA_PARALLELISM = """
## LPA distribuído: paralelismo por iteração

Cada iteração do Label Propagation usa um **snapshot síncrono** dos rótulos.
Os nós são particionados em chunks; todos os workers avaliam seus chunks **em
paralelo** (tasks Ray ou futures Dask lançados antes de qualquer coleta). O
driver faz merge dos rótulos e recalcula Q antes da próxima iteração.
""".strip()

REFERENCES = """
## Referências

1. Raghavan, U.N. et al. (2007). Near linear time algorithm to detect community structures in large-scale networks. *Physical Review E*, 76, 036106.
2. Leskovec & Krevl (2014). SNAP Datasets. https://snap.stanford.edu/data
3. Moritz et al. (2018). Ray: A Distributed Framework for Emerging AI Applications. OSDI 2018.
4. Rocklin (2015). Dask: Parallel Computation with Blocked algorithms and Task Scheduling. SciPy 2015.
5. Ray Core: https://docs.ray.io/en/latest/ray-core/walkthrough.html
6. Dask Distributed: https://distributed.dask.org/
""".strip()
