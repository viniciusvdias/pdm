# Final project report: Análise de séries temporais meterológicas do Brasil

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

- Describe the environment used for experiments (machine/VM specs, OS, Docker version, etc.).
- Example:
  
  > Experiments were run on a virtual machine with 4 vCPUs, 8GB RAM, Ubuntu 22.04, Docker 24.x.

### 6.2 How to perform benchmarking (simple guide)

**Step 1: Define what to measure**

- Choose metrics relevant to your workload:
  - **Execution time** (wall-clock time): Total time to complete the task
  - **Throughput**: Records/events processed per second
  - **Latency**: Time to process a single record/query
  - **Resource usage**: CPU, memory, disk I/O

**Step 2: Run multiple repetitions**

- **MANDATORY**: Run each experiment at least 3 times (preferably 5-10 times)
- This helps account for system variability and ensures reliable results
- Example command:
  
  ```bash
  for i in {1..5}; do
    echo "Run $i"
    docker compose up
    # For example, you stack may generate runtime statistics (metrics) in the logs
  done
  ```

**Step 3: Collect and record data**

- Create a spreadsheet or CSV file with your measurements
- Example data collection:
  
  ```
  Run 1: 45.2 seconds
  Run 2: 46.1 seconds
  Run 3: 44.8 seconds
  Run 4: 45.5 seconds
  Run 5: 45.9 seconds
  ```

**Step 4: Calculate statistics**

- **Average (mean)**: Sum all values and divide by number of runs
- **Standard deviation**: Measure of variability in your results
- Most tools (Excel, Python, R) can calculate these automatically
- Python example:
  
  ```python
  import numpy as np
  times = [45.2, 46.1, 44.8, 45.5, 45.9]
  avg = np.mean(times)
  std = np.std(times, ddof=1)  # Sample standard deviation
  print(f"Average: {avg:.2f}s ± {std:.2f}s")
  ```

**Step 5: Present results in tables**

- Use clear tables showing average ± standard deviation
- See examples in section 6.3 below

### 6.3 What did you test?

- What parameters did you vary? (e.g., data size, number of workers, batch size)
- What metrics did you measure? (execution time, throughput, memory usage, etc.)
- **MANDATORY**: For each configuration, run multiple repetitions (minimum 3) and report average and standard deviation

### 6.4 Results

**MANDATORY**: Results must include tables with average and standard deviation. Below are simple examples:

#### Example 1: Execution Time Comparison (not real measurements)

| Workload   | Configuration | Avg Time (s) | Std Dev (s) | Runs |
| ---------- | ------------- | ------------ | ----------- | ---- |
| WORKLOAD-1 | Single worker | 145.3        | 3.2         | 5    |
| WORKLOAD-1 | 2 workers     | 78.6         | 2.1         | 5    |
| WORKLOAD-1 | 4 workers     | 42.4         | 1.8         | 5    |
| WORKLOAD-2 | Single worker | 89.2         | 4.5         | 5    |
| WORKLOAD-2 | 2 workers     | 46.1         | 2.3         | 5    |

**Discussion**: Increasing the number of workers from 1 to 4 reduced execution time by ~70% for WORKLOAD-1. The low standard deviation (<5% of mean) indicates consistent performance.

#### Example 2: Throughput Analysis (not real measurements)

| Data Size (GB) | Throughput (MB/s) | Std Dev (MB/s) | Runs |
| -------------- | ----------------- | -------------- | ---- |
| 1              | 125.4             | 5.2            | 5    |
| 5              | 118.7             | 6.8            | 5    |
| 10             | 112.3             | 4.1            | 5    |
| 20             | 108.9             | 7.3            | 5    |

**Discussion**: Throughput decreased slightly (~13%) as data size increased from 1GB to 20GB, suggesting good scalability with minimal degradation.

#### Example 3: Resource Usage (not real measurements)

| Configuration    | Avg Memory (GB) | Std Dev (GB) | Peak CPU (%) | Runs |
| ---------------- | --------------- | ------------ | ------------ | ---- |
| Batch size 1000  | 2.4             | 0.15         | 78           | 3    |
| Batch size 5000  | 3.8             | 0.22         | 85           | 3    |
| Batch size 10000 | 5.2             | 0.31         | 92           | 3    |

**Discussion**: Memory usage scales linearly with batch size. Larger batches improve efficiency but require more memory.

#### Plots (recommended)

- Include bar charts or line plots showing your results
- Always include error bars representing standard deviation

**Key points for your results**:

- Always report both average AND standard deviation (or variance)
- Explain what the numbers mean in plain language
- Discuss trends, patterns, and unexpected results
- Compare different configurations or approaches
- Relate results back to your workload characteristics

## 7. Limitations and conclusions

- Summarize what worked and what did not.
- Discuss any challenges or limitations of this work.

## 8. References and external resources

- List all external resources, datasets, libraries, and tools you used (with links).
