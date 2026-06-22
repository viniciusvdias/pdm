# Final project report: Processamento em lote de dados meteorológicos do Brasil com chunking e conversão para Parquet

## 1. Context and motivation

- O objetivo deste projeto é processar um dataset meteorológico de 1.9GB utilizando a técnica de chunking (processamento em blocos) com Python e Pandas. Este trabalho resolve o problema da limitação de memória RAM ao manipular datasets massivos. O resultado é a conversão eficiente dos dados para o formato Parquet, otimizado para análise e consultas.

## 2. Data

### 2.1 Detailed description

- Fonte: [Climate Weather Surface of Brazil](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region?resource=download)

### 2.2 How to obtain the data

- Dataset de Amostra: Disponível na pasta datasample/ deste repositório.

- Dataset Completo: Para executar o processamento massivo, baixe o arquivo central_west.csv do [Climate Weather Surface of Brazil](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region?resource=download) e coloque-o na pasta data/ na raiz do projeto.

## 3. How to install and run

> Observation: The project must be compatible with a default Docker installation and use only Docker containers for running. No external tools or installations should be necessary — this is a strict requirement.

### 3.1 Quick start (using sample data in `datasample/`)

  ```bash
  ./bin/run.sh
  ```

### 3.2 How to run with the full dataset

- Coloque o arquivo central_west.csv na raiz do projeto, ajuste o caminho no arquivo main.py e  execute ./bin/run.sh. O script detectará automaticamente o arquivo completo e realizará o processamento.

## 4. Project architecture

A arquitetura do projeto foi desenhada para isolar o ambiente de execução e garantir a reprodutibilidade através do Docker. O fluxo de processamento é composto por três etapas principais:

 - Data Source (CSV): O arquivo de origem contendo as séries meteorológicas. É montado no container via Docker Volume para evitar o consumo de espaço no build da imagem.

 - Data Processing (Batch Layer): O container processor atua como a camada de processamento em lote. O script main.py utiliza a técnica de chunking (leitura em pedaços de 100k linhas), garantindo que o consumo de memória RAM permaneça estável, independentemente do tamanho total do arquivo de entrada.

 - Results Storage (Parquet): Os dados processados são convertidos e salvos no formato colunar Apache Parquet. Este formato foi escolhido por oferecer alta taxa de compressão e velocidade de leitura para futuras análises de Big Data.

## 5. Workloads evaluated

- Para este projeto, foram implementados e avaliados os seguintes workloads:

[WORKLOAD-1] Chunked Processing for Large Files: O core do projeto consiste em ler o arquivo CSV de 1.9GB utilizando a técnica de chunksize no Pandas. Isso evita que todo o arquivo seja carregado na RAM, prevenindo erros de estouro de memória (memory overflow).

[WORKLOAD-2] Batch Processing: O projeto opera sob um modelo de processamento em lote, onde o dataset é lido, transformado e persistido de forma estruturada, seguindo o cronograma da disciplina.

[WORKLOAD-3] Compression & Efficient Storage: O objetivo final é a conversão do formato CSV para Apache Parquet. Este workload é fundamental em Big Data para otimizar o espaço em disco e acelerar drasticamente a leitura dos dados em etapas analíticas futuras.

## 6. Experiments and results

> **MANDATORY**: This section is a core requirement of the project. You MUST perform experimental benchmarking with multiple repetitions and statistical analysis.

### 6.1 Experimental environment
- 16 GB RAM
- Processador i5 13450hx
- ubuntu 24.04

## 📊 Benchmark Results

| Chunk Size | Execution Times (s)                 | Mean (s)| Std Dev (s) | Parquet Size (MB) |
|------------|--------------------|----------|-------------|--------------------|
| 10,000     | 58.22 / 57.20 / 53.65 / 51.14 / 51.71 | 54.38 | 3.20 | 270.39 |
| 50,000     | 36.89 / 36.27 / 36.38 / 36.22 / 37.18 | 36.59 | 0.43 | 230.41 |
| 100,000    | 33.99 / 33.61 / 33.39 / 34.57 / 33.97 | 33.91 | 0.45 | 224.27 |

### 6.3 What did you test?

  O principal parâmetro variado nos experimentos foi o tamanho do chunk utilizado durante a leitura do arquivo CSV e a escrita do arquivo Parquet. Três configurações foram avaliadas:

  10.000 linhas por chunk
  50.000 linhas por chunk
  100.000 linhas por chunk

  Cada configuração foi executada 5 vezes utilizando o conjunto de dados completo (central_west.csv, aproximadamente 1,9 GB).

  As seguintes métricas foram medidas:

  Tempo de execução (em segundos) para cada execução
  Tempo médio de execução
  Desvio padrão do tempo de execução
  Tamanho total do arquivo Parquet gerado (MB)

O objetivo do experimento foi analisar como o tamanho do chunk afeta o desempenho e a estabilidade do pipeline de conversão de CSV para Parquet.

### 6.4 Discussion
  Os resultados mostram que chunks maiores melhoraram o desempenho da conversão de CSV para Parquet. A configuração com 100.000 linhas por chunk teve o melhor tempo médio de execução (33,91 s), enquanto a de 10.000 linhas foi a mais lenta (54,38 s).

  A estabilidade das execuções também variou. O chunk de 10.000 linhas apresentou maior desvio padrão (3,20 s), indicando desempenho menos consistente. Já as configurações de 50.000 e 100.000 linhas tiveram baixa variação (0,43 s e 0,45 s), mostrando execuções mais estáveis.

  Além disso, chunks maiores geraram arquivos Parquet menores. A configuração de 100.000 linhas produziu o menor arquivo (224,27 MB), enquanto a de 10.000 linhas gerou o maior (270,39 MB), sugerindo maior fragmentação e sobrecarga de metadados com chunks menores.

  No geral, o tamanho de 100.000 linhas por chunk apresentou o melhor equilíbrio entre tempo de execução, estabilidade e eficiência de armazenamento.


## 7. Limitations and conclusions

  Este trabalho demonstrou que o processamento em lote baseado em chunks é uma estratégia prática para lidar com grandes conjuntos de dados em formato CSV em ambientes com recursos limitados. O pipeline converteu com sucesso um conjunto de dados meteorológicos de 1,9 GB para o formato Apache Parquet, utilizando execução em Docker e experimentos reproduzíveis.

  Os resultados experimentais mostraram que o tamanho do chunk tem impacto direto tanto no tempo de execução quanto no tamanho do arquivo gerado. Chunks maiores levaram a melhor desempenho e a arquivos Parquet mais compactos no ambiente testado. Entre as configurações avaliadas, 100.000 linhas por chunk apresentou os melhores resultados.

  Como limitações, este projeto se concentra em um ambiente de processamento em nó único utilizando Pandas, o que não representa uma arquitetura distribuída de Big Data, como Apache Spark ou Apache Hadoop. Além disso, a carga de trabalho enfatiza a ingestão e a conversão de formato, em vez de tarefas analíticas posteriores sobre as séries meteorológicas processadas.

  Como trabalhos futuros, o pipeline pode ser estendido com 
  processamento paralelo, 
  frameworks de execução distribuída ou 
  consultas analíticas sobre os dados Parquet gerados.

## 8. References and external resources

- Climate Weather Surface of Brazil dataset: [kaggle.com](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region)
- Pandas documentation: [pandas.pydata.org](https://pandas.pydata.org/)
- Apache Parquet documentation: [parquet.apache.org](https://parquet.apache.org/)
- Docker documentation: [docs.docker.com](https://docs.docker.com/)