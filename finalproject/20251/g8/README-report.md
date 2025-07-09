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

  * One containing **traffic accident records** (30 columns)
  * Another containing **information about people involved** in those accidents (35 columns)

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

## 4. Project architecture

- Include a diagram of your system’s architecture and a description of how the components interact. Images are encouraged for clarity.
- What are the main components? How do they interact?
- Example diagram (replace with your own):

  ```
  [Data Source] → [Data Ingestion] → [Processing] → [Results Storage]
  ```

- Mention which parts run in which containers, and how data flows between them.

## 5. Workloads evaluated

- Specify each big data task evaluated in your project (queries, data pre-processing, sub-routines, etc.).
- Be specific: describe the details of each workload, and give each a clear name. These named workloads will be referenced and evaluated via performance experiments in the next section.
  - Example: [WORKLOAD-1] Query that computes the average occupation within each
    time window (include query below). [WORKLOAD-2] Preprocessing, including
  removing duplicates, standardization, etc.

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
