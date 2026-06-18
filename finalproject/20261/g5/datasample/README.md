# Datasample

Amostra sintética no formato MovieLens para testes rápidos.

| Arquivo | Registros | Tamanho |
|---|---|---|
| `movies.csv` | 100 filmes | ~4 KB |
| `ratings.csv` | 2.000 avaliações | ~44 KB |
| `tags.csv` | 400 tags | ~12 KB |

**Formato idêntico ao MovieLens 25M real.**

Para regenerar a amostra:
```bash
python bin/generate-sample.py
```

Para baixar o dataset completo (25M de avaliações, ~250MB):
```bash
bash bin/download-dataset.sh
```
