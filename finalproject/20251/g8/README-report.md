# Final Project Report: *Traffic Collisions Analytics*

## 1. Context and Motivation

* **What is the main goal? What problem are you trying to solve with big data?**

> The main goal is to analyze traffic collisions in order to identify patterns and trends.

> The problem addressed with big data is the difficulty of detecting meaningful patterns hidden within large volumes of raw traffic data. Big data tools are essential due to the scale of the dataset and the need to extract actionable insights through inference and pattern recognition.

## 2. Data

### 2.1 Detailed Description

* **Dataset source**
  The datasets used in this project are publicly available on the Brazilian Federal Government’s open data portal. Specifically, they are provided by the *Polícia Rodoviária Federal (PRF)*.
  📎 Source: [Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)

* **Dataset description**
  For each year from **2007 to 2025**, two datasets are provided:

  * One containing **traffic accident records** (30 columns) with 2.150.000 rows
  * Another containing **information about people involved** in those accidents (35 columns) with 4.600.000 rows
  * with approximately 6,8 million total rows.

* **Overview of key features**:

  **From the dataset of involved individuals:**

  * `data_inversa`: Enables temporal analysis and trend detection.
  * `tipo_veiculo`, `marca`, `ano_fabricacao_veiculo`: Help identify patterns by type and age of vehicles.
  * `tipo_envolvido`: Specifies the role in the accident (driver, passenger, pedestrian, etc.).
  * `estado_fisico`, `idade`, `sexo`: Allow demographic and consequence-related analysis.
  * `feridos_leves`, `feridos_graves`, `mortos`: Provide insights into accident severity.
  * `municipio`, `causa_acidente`: Can be cross-referenced with accident data.

  **From the dataset of accidents:**

  * `data_inversa`: Used for trend analysis over time.
  * `dia_semana`, `horario`: Capture weekly and hourly accident patterns.
  * `municipio`, `uf`, `br`, `km`, `latitude`, `longitude`: Enable geographic/spatial analysis.
  * `causa_acidente`: Key to identifying common causes.
  * `tipo_acidente`: Classifies the type of event (e.g., collision, pedestrian hit).
  * `classificacao_acidente`: Describes the severity (e.g., fatal, with injuries).
  * `fase_dia`, `condicao_metereologica`: Provide contextual information.
  * `mortos`, `feridos_leves`, `feridos_graves`, `ilesos`: Used to assess impact and outcomes.

### 2.2 How to Obtain the Data

> The dataset is available in a zipped folder hosted on Google Drive. Inside the archive, there is a folder named `full_data`, which contains two subfolders:
>
> * `ocorrencias`: contains accident data
> * `pessoas`: contains data on individuals involved in the accidents
>
> After downloading, the `full_data` folder should be **unzipped** and placed in the project repository at:
> `finalproject/20251/g8/full_data`
>
> 📎 [Download the dataset (Google Drive)](https://drive.google.com/file/d/14C-2ZmVKpcerKzkBSt-i0g3peIspz9p8/view?usp=sharing)

## 3\. How to Install and Run

The application runs on **Docker**, so you only need to have Docker installed.

-----

### 3.1 Quick Start (using sample data in `datasample/`)

To run the quick start, navigate to the `g8` folder. Then, grant execute permissions to the `start-compose.sh` file and pass `datasample` as an argument.

```bash
chmod +x ./start-compose.sh # Grant execute permission
./start-compose.sh datasample
````

-----

### 3.2 How to Run with the Full Dataset

To run with the full dataset, navigate to the `g8` folder. Grant execute permissions to the `start-compose.sh` file (if you haven't already), and pass `full_data` as an argument.

```bash
chmod +x ./start-compose.sh # Grant execute permission
./start-compose.sh full_data
```

## 4\. Project Architecture

The project was developed based on a containerized architecture using Docker, composed of three main services:

  * **JupyterLab**: An interactive environment for developing and running PySpark notebooks, hosted in the `jupyterlab` container.
  * **Spark Master**: The coordinator of the Apache Spark cluster, responsible for distributing tasks among the executors.
  * **Spark Worker(s)**: Executors responsible for processing the data on demand from the Spark Master.

### Architecture Diagram

```
[CSV Files in /app/full_data] 
      ↓ 
[JupyterLab Container] 
      ↓ imports code →
[Spark Master Container] ←→ [Spark Worker Container(s)]
      ↓ 
[Processed Results (DataFrames and Visualizations)] 
```

### Data Flow

1.  The CSV files from the `ocorrencias` and `pessoas` folders are stored in the shared volume at `/app/full_data`.
2.  The `JupyterLab` container runs scripts that import utility modules (`DataLoader`, `Workload`, etc.) and initialize the `SparkSession`.
3.  Data is loaded into distributed Spark DataFrames, processed by the Spark cluster (Master and Workers), and the results are displayed as visualizations within the Jupyter environment.
4.  Workloads encapsulate analysis blocks, standardizing execution and enabling performance reporting.

## 5\. Workloads Evaluated

Below are the main workloads evaluated in the project, along with their descriptions and representative code snippets:

-----

### \[WORKLOAD-1] Loading Accident Records

**Description:** Reads CSV files containing traffic accident records.

```python
df_ocorrencias = Workload.run(
    title="Loading accident records",
    execute_fn=lambda spark, path: executar_load(spark, path),
    spark=spark,
    path="/app/full_data/ocorrencias"
)
```

-----

### \[WORKLOAD-2] Loading Person Records

**Description:** Reads CSV files containing data on individuals involved in the accidents.

```python
df_pessoas = Workload.run(
    title="Loading person records",
    execute_fn=lambda spark, path: executar_load(spark, path),
    spark=spark,
    path="/app/full_data/pessoas"
)
```

-----

### \[WORKLOAD-3] Joining Accident and Person Data

**Description:** Performs a join between the two loaded DataFrames based on the `id` column.

```python
df_joined = Workload.run(
    title="Joining accidents and persons by ID",
    execute_fn=executar_join,
    df1=df_ocorrencias,
    df2=df_pessoas
)
```

-----

### \[WORKLOAD-4] Age Group Analysis

**Description:** Categorizes individuals by age group, aggregates the data, and generates a bar chart.

```python
Workload.run(
    title="Analyzing the number of individuals involved by age group",
    execute_fn=analise_faixa_etaria,
    df=df_pessoas
)
```

-----

### \[WORKLOAD-5] Gender Analysis

**Description:** Standardizes and categorizes the `sexo` (gender) field, with visualization in a pie chart.

```python
Workload.run(
    title="Analyzing the number of individuals involved by gender",
    execute_fn=analise_distribuicao_genero,
    df=df_pessoas
)
```

-----

### \[WORKLOAD-6] Temporal Distribution by Hour of the Day

**Description:** Extracts the hour from the `horario` field to analyze the volume of accidents by time of day.

```python
Workload.run(
    title="Temporal analysis of accidents: distribution by hour of the day",
    execute_fn=lambda df: analise_distribuicao_temporal(df, tipo="hora"),
    df=df_ocorrencias
)
```

-----

### \[WORKLOAD-7] Temporal Distribution by Day of the Week

**Description:** Analyzes accidents by day of the week with chronological ordering (Monday to Sunday).

```python
Workload.run(
    title="Temporal analysis of accidents: distribution by day of the week",
    execute_fn=lambda df: analise_distribuicao_temporal(df, tipo="semana"),
    df=df_ocorrencias
)
```

-----

### \[WORKLOAD-8] Temporal Distribution by Year

**Description:** Groups accidents based on the year extracted from the `data_inversa` field.

```python
Workload.run(
    title="Temporal analysis of accidents: distribution by year",
    execute_fn=lambda df: analise_distribuicao_temporal(df, tipo="ano"),
    df=df_ocorrencias
)
```

## 6\. Experiments and Results

### 6.1 Experimental Environment

Experiments were run on a physical machine with the following specifications:

  * **Motherboard:** Gigabyte Technology Co., Ltd. B250M-Gaming 3
  * **CPU:** Intel® Pentium(R) CPU G4560 @ 3.50GHz × 4
  * **RAM:** 12.0 GiB
  * **GPU:** NVIDIA Corporation GP107 [GeForce GTX 1050]

The project leverages **Docker and Docker Compose/Swarm** for a reproducible and isolated environment. The core of the big data processing is an **Apache Spark 4.0.0 cluster** configured in Standalone mode, using the `bitnami/spark:4.0.0` base image. Each Spark Worker was configured to use **2 CPU cores** and **2GiB of memory**. **JupyterLab with PySpark** serves as the interactive interface and Spark Driver, with a custom Docker image ensuring `pyspark` version 4.0.0 for compatibility. **Python 3.11** and libraries like **Pandas** and **Matplotlib** are used for final aggregation and visualization.

### 6.2 What did you test?

We tested the performance of the data pipeline by measuring the **execution time (in seconds)** for key stages: **data loading/caching** and **data processing**. We compared the use of **CSV files** against the optimized columnar format **Parquet**. The specific benchmark involved loading the entire dataset, caching it in memory, and then performing an aggregation analysis (by age group).

We evaluated the pipeline in two main environments:

1.  **Docker Compose (Single-node baseline):** This setup uses 1 Spark Master and 1 Spark Worker, representing a standard single-machine deployment.
2.  **Docker Swarm (Orchestrated environment):** This setup simulated a more production-like environment using 1 Master and 2 Workers running on the same physical machine to understand the impact of orchestration and increased parallelism.

The main parameters we varied were the **data format (CSV vs. Parquet)** and the **environment (Compose vs. Swarm)**. We measured the wall-clock time for each stage to quantify performance.

### 6.3 Results

#### Performance Results: Docker Compose (1 Worker)

The table below presents the execution of the pipeline in the single-node Docker Compose environment.

| Stage of the Pipeline | CSV (s) | Parquet (s) |
| :--- | :--- | :--- |
| 1. Loading and Caching | 38.172s | 58.528s |
| 2. Processing (Age Group) | 0.177s | 0.209s |

#### Performance Results: Docker Swarm (2 Workers)

The table below presents the execution of the pipeline in the orchestrated Swarm environment with 2 Spark Workers on the same physical machine.

| Stage of the Pipeline | CSV (s) | Parquet (s) |
| :--- | :--- | :--- |
| 1. Loading and Caching | 37.551s | 57.494s |
| 2. Processing (Age Group) | 0.162s | 0.276s |

#### Comparative Analysis: Docker Compose vs. Docker Swarm

| Stage (Format) | Compose (1 Worker) | Swarm (2 Workers) |
| :--- | :--- | :--- |
| Loading and Caching (CSV) | 38.172s | 37.551s |
| Loading and Caching (Parquet) | 58.528s | 57.494s |
| Processing (CSV) | 0.177s | 0.162s |
| Processing (Parquet) | 0.209s | 0.276s |

**Interpretation of Results:**

The experimental results revealed a counter-intuitive but insightful finding: for a full-scan operation like loading and caching the entire dataset, **CSV outperformed Parquet in total execution time**.

  * **CPU Trade-Offs (Decompression vs. Parsing):** The primary reason for this result lies in the trade-off between I/O and CPU costs. Parquet significantly reduces disk I/O due to its columnar nature and compression, but it pays a CPU penalty for decompressing the data upon reading. Conversely, CSV has a higher disk I/O cost (larger files) but pays a different CPU penalty for parsing the text-based format. In our specific environment (fast SSD, specific dataset characteristics), the CPU cost to decompress all columns from Parquet was higher than the cost to parse the CSV files.

  * **The Nature of the Benchmark:** It is crucial to note that this benchmark (`.cache().count()`) represents a "worst-case" scenario for Parquet, as it forces a full scan of all data and does not leverage Parquet's main advantage: **column pruning**. In typical analytical queries, where only a subset of columns is needed, Parquet would likely outperform CSV dramatically by reading only the required data from disk.

  * **Swarm vs. Compose:** The difference between the Compose and Swarm environments was minimal. A slight improvement was observed for CSV operations in Swarm, suggesting a minor optimization in I/O management by the orchestrator. However, with all containers competing for the same physical resources, the benefits of adding more workers were negligible. The true advantages of Swarm would only become apparent in a multi-node cluster.

-----

## 7\. Discussion and Conclusions

Our **Traffic Collisions Analytics** project successfully established a robust big data pipeline using Docker, Apache Spark, and JupyterLab to analyze extensive traffic accident datasets from the Brazilian Federal Government’s open data portal. The primary goal was to identify patterns and trends within these large volumes of raw traffic data, a challenge perfectly suited for big data methodologies.

### What Worked and What Did Not

  * **Containerized Architecture (Docker & Docker Compose/Swarm):** The use of Docker proved highly effective in creating a **reproducible and isolated environment**. This significantly streamlined setup and ensured consistent execution.
  * **Apache Spark for Big Data Processing:** Spark's distributed processing capabilities were crucial for handling the 6.8 million records. The abstraction of operations into `Workload` objects not only modularized the pipeline but also facilitated the extraction of performance metrics.
  * **Revealing Performance Trade-Offs:** The experiments, contrary to initial expectations, did not show a universal performance benefit for Parquet. Instead, they highlighted a critical concept in data engineering: performance is a function of the data's structure, the hardware, and the specific query being executed. For full-scan operations, the CPU overhead of Parquet's decompression proved to be a significant factor, leading to slower load times compared to CSV in this specific context. This finding, while unexpected, provided a deeper understanding of the formats' practical trade-offs.
  * **Data Pre-processing and Quality:** The implementation of robust pre-processing steps, such as handling invalid age and gender entries, was critical to ensure data quality and prevent errors during analysis.

### Challenges and Limitations

  * **Single-Node Swarm Environment:** A significant limitation was running Docker Swarm on a **single physical machine**. The workers competed for the same underlying CPU and memory resources, preventing true parallelism and masking the potential benefits of a distributed architecture.
  * **Benchmark Scope:** The primary benchmark involved a full data scan (`.cache().count()`), which is not representative of typical analytical queries. This specific test did not allow Parquet to demonstrate its key advantage of **column pruning**, where it would only read a small subset of the data from disk. Consequently, the performance results are specific to this "worst-case" scenario for columnar formats.
  * **Data Quality Issues:** The presence of a significant "Desconhecida" (Unknown) category for `idade` (age) and `sexo` (gender) highlighted persistent data quality challenges in the raw datasets, underscoring the need for improved data governance at the source.

### Future Directions

Future work could involve deploying the Docker Swarm setup on a **true multi-node cluster** to fully evaluate its scalability. A crucial next step would be to design and run benchmarks based on **analytical queries that select specific columns** (e.g., analyzing accident causes by vehicle type). Such tests would provide a more realistic comparison and would almost certainly demonstrate the superior performance of Parquet in scenarios where its column pruning capabilities are leveraged.

-----

## 8\. References and External Resources

  * **Dataset Source:** [Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)
  * **Dataset Download (Google Drive):** [https://drive.google.com/file/d/14C-2ZmVKpcerKzkBSt-i0g3peIspz9p8/view?usp=sharing](https://drive.google.com/file/d/14C-2ZmVKpcerKzkBSt-i0g3peIspz9p8/view?usp=sharing)
  * **Docker:** [https://www.docker.com/](https://www.docker.com/)
  * **Apache Spark:** [https://spark.apache.org/](https://spark.apache.org/)
  * **JupyterLab:** [https://jupyter.org/](https://jupyter.org/)
  * **PySpark Documentation:** [https://spark.apache.org/docs/latest/api/python/index.html](https://spark.apache.org/docs/latest/api/python/index.html)
  * **Bitnami Spark Docker Image:** [https://hub.docker.com/r/bitnami/spark](https://hub.docker.com/r/bitnami/spark)