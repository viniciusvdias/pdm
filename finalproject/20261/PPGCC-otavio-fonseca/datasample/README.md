# Amostra de dados — NYC Yellow Taxi (Jun/2024)

Primeiras 200 linhas (cabeçalho + 199 corridas) do arquivo `yellow_tripdata_2024-06.csv`,
que é 1 dos ~30 arquivos mensais usados neste projeto.

- **Tamanho:** ~21 KB (limite do template: 1 MB)
- **Colunas:** 19 — VendorID, timestamps de embarque/desembarque, distância, passageiros, tarifas, taxas, etc.
- **Fonte original:** [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Testar rapidamente com esta amostra

```bash
# A partir da raiz do grupo (PPGCC-otavio-fonseca/)
./bin/run.sh sample
```

Ou manualmente com Docker Compose:

```bash
# A partir da pasta PPGCC-otavio-fonseca/
cd bigdata-preprocessing
docker compose run --rm \
  -v "$(pwd)/../datasample:/datasample" \
  -e DATASET_PATH=/datasample/taxi_sample.csv \
  -e RESULTS_PATH=/results/metrics_sample.csv \
  benchmark
```

> O serviço `benchmark` não monta `datasample/` por padrão — por isso o
> `-v "$(pwd)/../datasample:/datasample"` é necessário para expor a amostra
> dentro do container.

## Obter o dataset completo

Execute o serviço `downloader` para baixar os arquivos Parquet do NYC TLC e gerar os CSVs de 1 GB, 5 GB e 10 GB:

```bash
cd bigdata-preprocessing
docker compose --profile setup up downloader
```

Ver instruções completas no [README principal](../README.md#22-how-to-obtain-the-data).
