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
```

-----

### 3.2 How to Run with the Full Dataset

To run with the full dataset, navigate to the `g8` folder. Grant execute permissions to the `start-compose.sh` file (if you haven't already), and pass `full_data` as an argument.

```bash
chmod +x ./start-compose.sh # Grant execute permission
./start-compose.sh full_data
```

## 4. Project Architecture

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

1. The CSV files from the `ocorrencias` and `pessoas` folders are stored in the shared volume at `/app/full_data`.
2. The `JupyterLab` container runs scripts that import utility modules (`DataLoader`, `Workload`, etc.) and initialize the `SparkSession`.
3. Data is loaded into distributed Spark DataFrames, processed by the Spark cluster (Master and Workers), and the results are displayed as visualizations within the Jupyter environment.
4. Workloads encapsulate analysis blocks, standardizing execution and enabling performance reporting.

## 5. Workloads Evaluated

Below are the main workloads evaluated in the project, along with their descriptions and representative code snippets:

---

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

---

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

---

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

---

### \[WORKLOAD-4] Age Group Analysis

**Description:** Categorizes individuals by age group, aggregates the data, and generates a bar chart.

```python
Workload.run(
    title="Analyzing the number of individuals involved by age group",
    execute_fn=analise_faixa_etaria,
    df=df_pessoas
)
```

---

### \[WORKLOAD-5] Gender Analysis

**Description:** Standardizes and categorizes the `sexo` (gender) field, with visualization in a pie chart.

```python
Workload.run(
    title="Analyzing the number of individuals involved by gender",
    execute_fn=analise_distribuicao_genero,
    df=df_pessoas
)
```

---

### \[WORKLOAD-6] Temporal Distribution by Hour of the Day

**Description:** Extracts the hour from the `horario` field to analyze the volume of accidents by time of day.

```python
Workload.run(
    title="Temporal analysis of accidents: distribution by hour of the day",
    execute_fn=lambda df: analise_distribuicao_temporal(df, tipo="hora"),
    df=df_ocorrencias
)
```

---

### \[WORKLOAD-7] Temporal Distribution by Day of the Week

**Description:** Analyzes accidents by day of the week with chronological ordering (Monday to Sunday).

```python
Workload.run(
    title="Temporal analysis of accidents: distribution by day of the week",
    execute_fn=lambda df: analise_distribuicao_temporal(df, tipo="semana"),
    df=df_ocorrencias
)
```

---

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

Experiments were run on a virtual machine with **4 vCPUs**, **8GB RAM**, **Ubuntu 22.04**, and **Docker version 24.x**. The project leverages **Docker and Docker Compose** for a reproducible and isolated development environment. The core of the big data processing is an **Apache Spark 4.0.0 cluster** configured in Standalone mode with 1 Master and 1 Worker, using the `bitnami/spark:4.0.0` base image. **JupyterLab with PySpark** serves as the interactive interface and Spark Driver, with a custom Docker image ensuring `pyspark` version 4.0.0 for compatibility. **Python 3.11** and libraries like **Pandas** and **Matplotlib** are used for final aggregation and visualization.

### 6.2 What did you test?

We tested the performance of the data pipeline by measuring the **execution time (in seconds)** for key stages: **data loading, processing, and result writing**. We compared the use of **CSV files** against the optimized columnar format **Parquet**. For each configuration, experiments were repeated **5 times** to report the average execution time.

We evaluated the pipeline in two main environments:

1.  **Docker Compose (Single-node baseline):** This setup represents a standard single-machine deployment, providing a baseline for performance.
2.  **Docker Swarm (Orchestrated environment):** This setup simulated a more production-like environment. Initially, we used 1 Master and 2 Workers running on the **same physical machine** to understand the overhead of orchestration. Subsequently, we scaled the Spark Workers to **4 nodes** within the same single physical machine to observe the impact of increased parallelization within the simulated environment.

The main parameters we varied were the **data format (CSV vs. Parquet)** and the **number of Spark Workers (1, 2, and 4)** in the Docker Swarm setup. We measured the wall-clock time for each stage to quantify the performance gains.

### 6.3 Results

#### Performance Results: Docker Compose (Single-Node Baseline)

The table below presents the average of 5 executions of the pipeline in the single-node Docker Compose environment.

| Stage of the Pipeline   | CSV (s) | Parquet (s) | Gain (Parquet)     |
| :---------------------- | :------ | :---------- | :----------------- |
| 1. Data Loading         | 5.345s  | 0.048s      | **111.3x faster** |
| 2. Processing           | 0.038s  | 0.031s      | **1.2x faster** |
| 3. Result Writing       | 4.679s  | 0.679s      | **6.9x faster** |

These results serve as our baseline for comparison with the orchestrated environment. The most significant gain is observed in **data loading**, where Parquet is over 100 times faster than CSV, highlighting the efficiency of columnar storage. Writing results to Parquet is also significantly faster. Processing time shows a smaller but still notable improvement.

#### Performance Results: Docker Swarm (2 Workers on Single Node)

The table below presents the average of 5 executions of the pipeline in the orchestrated Swarm environment with 2 Spark Workers, all running on the same physical machine.

| Stage of the Pipeline   | CSV (s) | Parquet (s) | Gain (Parquet)     |
| :---------------------- | :------ | :---------- | :----------------- |
| 1. Data Loading         | 3.127s  | 0.071s      | **44x faster** |
| 2. Processing           | 0.043s  | 0.031s      | **1.4x faster** |
| 3. Result Writing       | 2.708s  | 0.730s      | **3.7x faster** |

Comparing these results to the Docker Compose baseline reveals some interesting insights. While Parquet still offers substantial gains over CSV, the performance difference for loading and writing is slightly less pronounced in the Swarm environment. This indicates that the **orchestration layer introduces a small overhead**, which becomes more noticeable for very fast operations. However, for CSV loading and writing, Swarm still performs better than Compose, suggesting that the Swarm scheduler might be more efficient in managing I/O resources in a multi-container setup, even on a single machine.

#### Performance Results: Docker Swarm (4 Workers on Single Node)

The table below presents the average of 5 executions of the pipeline in the orchestrated Swarm environment with **4 Spark Workers**, all running on the same physical machine.

| Stage of the Pipeline   | CSV (s) | Parquet (s) | Gain (Parquet)     |
| :---------------------- | :------ | :---------- | :----------------- |
| 1. Data Loading         | 2.364s  | 0.079s      | **29.9x faster** |
| 2. Processing           | 0.038s  | 0.029s      | **1.3x faster** |
| 3. Result Writing       | 1.890s  | 0.877s      | **2.2x faster** |

With 4 workers, the overall performance for CSV loading and writing continues to improve compared to the 2-worker Swarm and Compose setups. However, the **gain of Parquet over CSV is slightly reduced** for loading. This reinforces the idea that increasing the number of workers on a single physical machine can lead to resource contention, impacting the absolute performance benefits even for optimized formats. The processing time with Parquet remains consistently fast, demonstrating its efficiency regardless of the number of workers on a single node.

#### Comparative Analysis: Docker Compose vs. Docker Swarm

| Stage (Format)              | Compose (s) | Swarm (s) (2 Workers) | Swarm (s) (4 Workers) |
| :-------------------------- | :---------- | :-------------------- | :-------------------- |
| Data Loading (CSV)          | 5.345s      | 3.127s                | 2.364s                |
| Data Loading (Parquet)      | 0.048s      | 0.071s                | 0.079s                |
| Processing (CSV)            | 0.038s      | 0.043s                | 0.038s                |
| Processing (Parquet)        | 0.031s      | 0.031s                | 0.029s                |
| Result Writing (CSV)        | 4.679s      | 2.708s                | 1.890s                |
| Result Writing (Parquet)    | 0.679s      | 0.730s                | 0.877s                |

**Interpretation of Results:**

  * **Parquet's Dominance:** The most consistent and significant finding is the overwhelming performance advantage of **Parquet** over CSV across all stages and environments. This validates the importance of using optimized data formats for big data workloads, drastically accelerating analysis cycles.
  * **Swarm Overhead vs. Optimization:** For very quick operations, like Parquet loading, the Docker Compose (single-node) setup was slightly faster than Docker Swarm. This suggests that the orchestration and networking layer of Swarm introduces a small **overhead** that is perceptible in millisecond-range tasks. However, for the slowest tasks, such as CSV loading and writing, the Swarm environment (with both 2 and 4 workers) showed notable improvements compared to Docker Compose. This might indicate that the Swarm scheduler optimizes I/O resource allocation more efficiently in a multi-container setup, even when running on a single machine.
  * **Scalability on a Single Node:** While increasing workers from 2 to 4 on the same physical machine generally reduced CSV processing times, the gains for Parquet were less pronounced and sometimes even showed a slight increase in loading/writing times. This highlights the limitations of scaling horizontally on a single machine due to shared resource contention (CPU, RAM, disk I/O). The true performance benefits of Docker Swarm would be realized in a **multi-node cluster**, where real parallelism could overcome any initial orchestration overhead. The tests on a single machine serve as a valuable simulation of the architecture's viability and the impact of worker replication.

-----

## 7\. Discussion and Conclusions

Our **Traffic Collisions Analytics** project successfully established a robust big data pipeline using Docker, Apache Spark, and JupyterLab to analyze extensive traffic accident datasets from the Brazilian Federal Government’s open data portal. The primary goal was to identify patterns and trends within these large volumes of raw traffic data, a challenge perfectly suited for big data methodologies.

### What Worked and What Did Not

  * **Containerized Architecture (Docker & Docker Compose/Swarm):** The use of Docker proved highly effective in creating a **reproducible and isolated environment**. This significantly streamlined setup and ensured consistent execution across different environments. Docker Compose provided a quick and easy way to spin up the Spark cluster for development and baseline testing.
  * **Apache Spark for Big Data Processing:** Spark's distributed processing capabilities were crucial for handling the 6.8 million records, enabling efficient data loading, transformation, and analysis. The abstraction of operations into `Workload` objects not only modularized the pipeline but also facilitated performance metric extraction, a key aspect of our experimental evaluation.
  * **Data Pre-processing and Quality:** The implementation of robust pre-processing steps, such as handling invalid age and gender entries, and standardizing categorical data, was critical. This ensured data quality and prevented errors during analysis, demonstrating the importance of data cleaning in real-world big data projects.
  * **Performance Benefits of Parquet:** The experiments consistently showed a dramatic performance improvement when using Parquet over CSV for data loading and writing. This validated the strategic decision to prioritize optimized columnar formats in the data pipeline, significantly accelerating analytical cycles.
  * **Docker Swarm for Orchestration:** While our Swarm tests were limited to a single physical machine, they successfully demonstrated the viability of the orchestrated architecture. The Swarm scheduler showed potential for optimizing resource allocation, especially for I/O-bound tasks. This setup provides a solid foundation for future deployment on a true multi-node cluster, where the benefits of distributed processing and high availability would be fully realized.

### Challenges and Limitations

  * **Single-Node Swarm Environment:** A significant limitation of our experiments was running Docker Swarm on a **single physical machine**. While this allowed us to simulate the architecture and observe the overhead of orchestration, it did not fully showcase the benefits of true distributed computing and parallel processing that would come from a multi-node cluster. The workers, despite being separate containers, competed for the same underlying CPU and memory resources, leading to potential resource contention that might mask the full performance gains of increased parallelism.
  * **Data Quality Issues:** Despite implementing robust pre-processing, the presence of a significant "Desconhecida" (Unknown) category for `idade` (age) and `sexo` (gender) highlighted persistent data quality challenges in the raw datasets. This underscores the need for continuous data governance and improved data collection practices at the source.
  * **Scope of Analysis:** While we performed several key demographic and temporal analyses, the project's scope was limited to these specific workloads. A more comprehensive analysis could involve geospatial analysis, machine learning for accident prediction, or deeper correlation of various accident factors.
  * **Lack of Fault Tolerance Testing:** Although Docker Swarm inherently provides features for high availability, our experiments did not explicitly test the fault tolerance or self-healing capabilities of the system (e.g., simulating a worker failure).

### Future Directions

Future work could involve deploying the Docker Swarm setup on a **true multi-node cluster** to fully evaluate its scalability and distributed processing capabilities. Exploring additional Spark features like Spark Streaming for real-time analysis or integrating with other big data tools (e.g., Kafka for data ingestion, Hadoop HDFS for storage) would further enhance the project. Moreover, developing a user-friendly dashboard for interactive visualization of the analytical insights would increase the project's utility for stakeholders.

-----

## 8\. References and External Resources

  * **Dataset Source:** [Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)
  * **Dataset Download (Google Drive):** [https://drive.google.com/file/d/14C-2ZmVKpcerKzkBSt-i0g3peIspz9p8/view?usp=sharing](https://drive.google.com/file/d/14C-2ZmVKpcerKzkBSt-i0g3peIspz9p8/view?usp=sharing)
  * **Docker:** [https://www.docker.com/](https://www.docker.com/)
  * **Apache Spark:** [https://spark.apache.org/](https://spark.apache.org/)
  * **JupyterLab:** [https://jupyter.org/](https://jupyter.org/)
  * **PySpark Documentation:** [https://spark.apache.org/docs/latest/api/python/index.html](https://spark.apache.org/docs/latest/api/python/index.html)
  * **Bitnami Spark Docker Image:** [https://hub.docker.com/r/bitnami/spark](https://hub.docker.com/r/bitnami/spark)