# Final project report: Análise de séries temporais meteorológicas do Brasil

## 1. Context and motivation

* O objetivo deste projeto é processar um dataset meteorológico de 1.9GB utilizando a técnica de chunking (processamento em blocos) com Python e Pandas. Este trabalho resolve o problema da limitação de memória RAM ao manipular datasets massivos. O resultado é a conversão eficiente dos dados para o formato Parquet, otimizado para análise e consultas.

* Embora o dataset não esteja na escala de dezenas de terabytes, ele apresenta desafios típicos de Big Data relacionados ao volume dos dados. O arquivo possui aproximadamente 1.9GB e exige estratégias específicas de processamento para evitar o carregamento integral dos dados em memória.

* O projeto aborda principalmente o conceito de **Volume**, um dos pilares clássicos do Big Data, demonstrando como técnicas de processamento em chunks permitem manipular grandes volumes de dados utilizando recursos computacionais limitados.

## 2. Data

### 2.1 Detailed description

* Fonte: [Climate Weather Surface of Brazil](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region?resource=download)

* O dataset contém séries temporais meteorológicas do Brasil, incluindo variáveis como:

  * Temperatura
  * Umidade
  * Pressão atmosférica
  * Precipitação
  * Velocidade do vento
  * Data e hora das medições

* Tamanho aproximado do dataset utilizado: **1.9 GB**

### 2.2 How to obtain the data

* Dataset de Amostra: Disponível na pasta `datasample/` deste repositório.

* Dataset Completo: Para executar o processamento massivo, baixe o arquivo `central_west.csv` do [Climate Weather Surface of Brazil](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region?resource=download) e coloque-o na pasta `data/` na raiz do projeto.

## 3. How to install and run

> Observation: The project must be compatible with a default Docker installation and use only Docker containers for running. No external tools or installations should be necessary — this is a strict requirement.

### 3.1 Quick start (using sample data in `datasample/`)

```bash
./bin/run.sh
```

### 3.2 How to run with the full dataset

* Coloque o arquivo `central_west.csv` na raiz do projeto.
* Ajuste o caminho no arquivo `main.py`, se necessário.
* Execute:

```bash
./bin/run.sh
```

O script iniciará automaticamente o ambiente Docker e executará todos os experimentos definidos no benchmark.

## 4. Project architecture

A arquitetura do projeto foi desenhada para isolar o ambiente de execução e garantir a reprodutibilidade através do Docker.

Fluxo de processamento:

```text
Dataset CSV (1.9 GB)
          |
          v
    Docker Container
          |
          v
   Leitura em Chunks
          |
          v
 Conversão com Pandas
          |
          v
 Arquivos Apache Parquet
          |
          v
 Diretório de Saída
```

### Data Source (CSV)

O arquivo de origem contendo as séries meteorológicas é disponibilizado ao container via Docker Volume, evitando cópias desnecessárias durante a construção da imagem.

### Data Processing (Batch Layer)

O container `processor` atua como a camada de processamento em lote.

O script principal utiliza a técnica de chunking para processar o dataset em blocos de diferentes tamanhos:

* 10.000 linhas
* 50.000 linhas
* 100.000 linhas

Essa abordagem permite controlar o consumo de memória RAM e avaliar o impacto do tamanho dos chunks sobre o desempenho.

### Results Storage (Parquet)

Os dados processados são convertidos para o formato Apache Parquet.

O formato Parquet foi escolhido por oferecer:

* Compressão eficiente;
* Menor consumo de armazenamento;
* Leitura otimizada para análises futuras;
* Compatibilidade com ferramentas de Big Data e Data Lakes.

## 5. Workloads evaluated

Para este projeto, foram implementados e avaliados os seguintes workloads:

### [WORKLOAD-1] Chunked Processing for Large Files

O núcleo da solução consiste na leitura do arquivo CSV utilizando a funcionalidade de chunking do Pandas.

Essa estratégia permite processar datasets de grande porte sem necessidade de carregá-los integralmente na memória RAM.

### [WORKLOAD-2] Batch Processing

O projeto segue o paradigma de processamento em lote.

O dataset é lido, transformado e persistido em uma única execução controlada.

### [WORKLOAD-3] Compression & Efficient Storage

Após o processamento, os dados são convertidos para Apache Parquet.

Essa etapa reduz significativamente o espaço ocupado em disco e melhora o desempenho de futuras consultas analíticas.

### [WORKLOAD-4] Format Conversion

Conversão de dados do formato CSV para o formato colunar Apache Parquet, simulando uma etapa comum de ingestão e preparação de dados para ambientes de Data Lake.

## 6. Experiments and results

### 6.1 Experimental environment

Os experimentos foram executados utilizando:

* Sistema Operacional: Ubuntu Linux
* Linguagem: Python 3.9
* Bibliotecas:

  * Pandas
  * PyArrow
* Containerização:

  * Docker
  * Docker Compose

### 6.2 How to perform benchmarking (simple guide)

O benchmark é executado automaticamente durante a execução do container.

Foram avaliadas três configurações distintas de chunk size:

| Configuração | Chunk Size |
| ------------ | ---------- |
| A            | 10.000     |
| B            | 50.000     |
| C            | 100.000    |

Cada configuração foi executada 5 vezes.

As métricas avaliadas foram:

* Tempo de execução;
* Média das execuções;
* Desvio padrão;
* Tamanho final dos arquivos Parquet.

### 6.3 What did you test?

O objetivo dos experimentos foi avaliar o impacto do tamanho do chunk sobre o desempenho do processamento.

Hipótese avaliada:

* Chunks menores consomem menos memória RAM, porém realizam mais operações de leitura, escrita e criação de DataFrames.
* Chunks maiores tendem a ser mais rápidos, porém exigem maior quantidade de memória RAM.

O experimento buscou identificar o equilíbrio entre desempenho e utilização de recursos.

### 6.4 Results

#### Resultados Consolidados

| Chunk Size | Média (s) | Desvio Padrão (s) | Tamanho Parquet (MB) | Execuções |
| ---------- | --------- | ----------------- | -------------------- | --------- |
| 10.000     | 48.39     | 3.40              | 270.39               | 5         |
| 50.000     | 32.15     | 0.43              | 230.41               | 5         |
| 100.000    | 25.18     | 0.27              | 224.27               | 5         |

#### Resultados detalhados

##### Chunk Size = 10.000

| Execução | Tempo (s) |
| -------- | --------- |
| 1        | 52.10     |
| 2        | 50.98     |
| 3        | 48.37     |
| 4        | 46.99     |
| 5        | 43.51     |

##### Chunk Size = 50.000

| Execução | Tempo (s) |
| -------- | --------- |
| 1        | 32.56     |
| 2        | 32.42     |
| 3        | 32.16     |
| 4        | 32.12     |
| 5        | 31.45     |

##### Chunk Size = 100.000

| Execução | Tempo (s) |
| -------- | --------- |
| 1        | 24.89     |
| 2        | 25.56     |
| 3        | 25.11     |
| 4        | 25.34     |
| 5        | 24.99     |

#### Discussion

Os resultados demonstram claramente o impacto do tamanho do chunk sobre o desempenho.

Observou-se que:

* Chunks menores apresentaram maior tempo de execução;
* Chunks maiores apresentaram melhor desempenho;
* O desvio padrão diminuiu conforme o tamanho do chunk aumentou, indicando maior estabilidade das execuções.

O chunk de 100.000 linhas apresentou o melhor resultado:

* Menor tempo médio de execução (25.18 s);
* Menor variabilidade entre execuções (0.27 s);
* Menor tamanho final dos arquivos Parquet.

Esse comportamento ocorre porque chunks maiores reduzem a quantidade de operações de leitura e escrita em disco, bem como a quantidade de DataFrames criados pelo Pandas.

Por outro lado, chunks maiores exigem mais memória RAM durante o processamento.

Os experimentos demonstram o clássico trade-off entre desempenho e consumo de memória encontrado em sistemas de Big Data.

## 7. Limitations and conclusions

O principal objetivo do projeto foi alcançado: processar um dataset meteorológico de grande porte utilizando recursos computacionais limitados.

A técnica de chunking permitiu processar aproximadamente 1.9GB de dados sem necessidade de carregamento integral do arquivo em memória.

Os experimentos demonstraram que o tamanho do chunk possui impacto direto sobre o desempenho do processamento.

As principais conclusões foram:

* Chunks menores consomem menos memória RAM;
* Chunks maiores reduzem significativamente o tempo de execução;
* Existe um equilíbrio entre desempenho e uso de recursos computacionais;
* O formato Parquet proporciona armazenamento mais eficiente e melhor preparação dos dados para análises futuras.

Como trabalhos futuros, poderiam ser avaliadas soluções distribuídas como:

* Apache Spark;
* Dask;
* Ray;

bem como comparações entre processamento local e processamento distribuído.

## 8. References and external resources

### Dataset

* Climate Weather Surface of Brazil:
  https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region

### Technologies

* Python 3.9
* Pandas
* PyArrow
* Docker
* Docker Compose
* Apache Parquet

### Documentation

* Pandas Documentation:
  https://pandas.pydata.org/

* Apache Parquet:
  https://parquet.apache.org/

* Docker Documentation:
  https://docs.docker.com/
