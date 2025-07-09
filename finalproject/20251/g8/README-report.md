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

## 3. How to install and run

> Observation: The project must be compatible with a default Docker installation and use only Docker containers for running. No external tools or installations should be necessary — this is a strict requirement.

### 3.1 Quick start (using sample data in `datasample/`)

- Give the exact command(s) to run your project out of the box using Docker or your scripts.
- Example:

  ```bash
  docker compose up --build
  # or if using a script:
  ./bin/run.sh
  ```

### 3.2 How to run with the full dataset

- Explain clearly how to configure or mount the full dataset (if different from default sample).

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

## 6. Experiments and results

### 6.1 Experimental environment

- Describe the environment used for experiments (machine/VM specs, OS, Docker version, etc.).
- Example:
  > Experiments were run on a virtual machine with 4 vCPUs, 8GB RAM, Ubuntu 22.04, Docker 24.x.

### 6.2 What did you test?

- What parameters did you try changing? What did you measure (e.g. throughput, latency, wall-clock time, resident memory, disk usage, application level metrics)?
- The ideal is that, for each execution configuration, you repeat the experiments a number of times (replications). With this information, report the average and also the variance/deviation of the results.

### 6.3 Results

- Use tables and plots (insert images).
- Discuss your results: What do they mean? What did you learn about the data or
about the computational cost of processing this data?
- Do not just show numbers or plots — always explain what they mean and why they matter.

## 7. Discussion and conclusions

- Summarize what worked and what did not.
- Discuss any challenges or limitations of this work.

## 8. References and external resources

- List all external resources, datasets, libraries, and tools you used (with links).
