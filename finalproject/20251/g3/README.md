# Final project report: *Historical Analysis of Fuel Prices in Brazil (2004-2019) Using ANP Data*

## 1. Context and motivation

The main goal of this project is to analyze fuel price behavior in Brazil over time using Big Data technologies. We aim to extract insights about price trends, regional differences, and political influences on fuel costs by processing a large volume of historical data provided by the ANP (National Agency of Petroleum, Natural Gas and Biofuels).

By leveraging Big Data tools like PySpark, we are able to:

- Efficiently handle dozens of CSV files spanning over two decades,

- Perform data cleaning, transformation, and enrichment at scale,

- Structure the data into a dimensional model (data lake),

- Enable visual analytics and aggregated reports based on product type, semester, and political mandates.

This project addresses the common challenge of extracting meaningful, reliable insights from raw government datasets, which are often large, inconsistent, and semi-structured. Through this pipeline, we transform raw fuel price records into a refined, trustworthy dataset suitable for data-driven decision making and public policy evaluation.

## 2. Data

### 2.1 Detailed description

The dataset used in this project was obtained from the Brazilian National Agency of Petroleum, Natural Gas and Biofuels (ANP), which regularly publishes historical data on fuel sales in Brazil. It has data from 2004 till 2021

The specific files are available in CSV format under the section for "Revenda – Preços Médios Mensais de Combustíveis por Município."

[link](https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis-e-de-glp).

### 2.2 How to obtain the data

The project execution data consists of CSV files hosted externally, named using the following pattern:

```
ca-{ano}-0{semestre}.csv
```

Onde:

- {ano} ranges from 2004 to the current year

- {semestre} can be either 1 or 2

Example of a valid filename:

- `ca-2010-01.csv`

To simplify the download and organization of these files, a script has been containerized using Docker. Follow the steps below to run it:

1. Navigate to the script directory:

``` sh
# If you are in the project root directory
cd src/raw-script
```

2. Build the Docker image:

``` sh
docker build -t raw-app .
```

3. Run the generated container:

> Note: This process may take a few minutes depending on your internet connection and machine performance.

``` sh
docker run --rm -it -v $(pwd)/../datalake:/app/datalake raw-app
```

## 3. How to install and run

> Observation: The project must be compatible with a default Docker installation and use only Docker containers for running. No external tools or installations should be necessary — this is a strict requirement.

### 3.1 Quick start (using sample data in `datasample/`)

To run the project using the sample data, copy the CSV file from `/datasample` to the `/src/datalake/raw/` directory as shown in the steps below:

1. Create the `/src/datalake/raw/` directory (if it doesn't exist already):

``` sh
mkdir -p src/datalake/raw/
```

2. Copy the CSV file from `datasample` to `/src/datalake/raw/`:

``` sh
cp datasample/datasample.csv /src/datalake/raw/datasample.csv
```

Now that the `datasample.csv` is in the correct directory, run the processing script:

1. Navigate to the script directory:

``` sh
# If you are in the project root directory
cd src/process-script
```

2. Build the Docker image:

``` sh
docker build -t process-app .
```

3. Run the generated container:

``` sh
docker run --rm -it -v $(pwd)/../datalake:/app/datalake process-app
```

### 3.2 How to run with the full dataset

To run the project with the complete dataset, use the Dockerfiles located at [src/raw-script/Dockerfile](src/raw-script/Dockerfile) and [src/process-script/Dockerfile](src/process-script/Dockerfile).

The Dockerfile in `src/raw-script/` is responsible for fetching the entire dataset and saving it to `src/datalake/raw`.
The Dockerfile in `src/process-script/` executes the processing script and generates the results in `src/datalake/plots/`.

To run these two Dockerfiles, follow the step-by-step instructions below.

1. Navigate to the raw script directory:

``` sh
# If you are in the project root directory
cd src/raw-script
```

2. Build the raw Docker image:

``` sh
docker build -t raw-app .
```

3. Run the generated container:

> Note: This process may take a few minutes depending on your internet connection and machine performance.

``` sh
docker run --rm -it -v $(pwd)/../datalake:/app/datalake raw-app
```

4. Navigate to the process script directory:

``` sh
cd ../process-script
```

5. Build the process Docker image:

``` sh
docker build -t process-app .
```

6. Run the generated container:

``` sh
docker run --rm -it -v $(pwd)/../datalake:/app/datalake process-app
```

## 4. Project architecture

### 4.1. Component

- Data Source: Public CSV files from [ANP](https://www.gov.br/anp) with fuel prices.
- Data Ingestion: Uses Python's `subprocess` + `wget` to download files into `datalake/raw`.
- Processing Engine: `PySpark` performs ETL: cleaning, enrichment, modeling.
- Storage: Files are stored in three layers: `raw`, `trusted`, and `refined` (Parquet).
- Analysis Layer: Uses `matplotlib`, `pandas` and `PySpark` queries for visual reports.

### 4.2. Diagram

#### 4.2.1. Process diagram
```
[ANP CSV Files]
      │
      ▼
🗂️ [Raw Zone - datalake/raw]
  ⤷ Raw CSV files are downloaded and stored here
      │
      ▼
🧼 [Trusted Zone - datalake/trusted]
  ⤷ Data cleaning, type conversion, column removal
      │
      ▼
🏗️ [Refined Zone - datalake/refined]
  ⤷ Data modeling: dimensions + fact tables (star schema)
      │
      ▼
📊 [Analysis & Visualization]
  ⤷ Plots, average metrics by semester and political mandate
```

#### 4.2.2. ayers diagram:

![](/src/img/project-diagram.png)

### 4.2. Divisão dos containers

The entire project runs using just two isolated containers.

#### 4.2.1. raw-app

This container has its Docker image built from the [raw Dockerfile](/src/raw-script/Dockerfile) and is responsible for running the [main.py](/src/raw-script/main.py) script, located in the `/src/raw-script` directory.

The script performs a series of requests to open data from the `gov.br` portal, where ANP files are hosted. The retrieved files are saved to the `datalake/raw` directory.

During execution via Docker, the local `../datalake` directory is mounted to `/app/datalake` inside the container. This is done using the following command:

``` sh
docker run --rm -it -v $(pwd)/../datalake:/app/datalake raw-app
```

#### 4.2.2. process-app

This second service has its Docker image built from the [script Dockerfile](/src/process-script/Dockerfile) and is responsible for processing all the data using the [main.py](/src/process-script/main.py) script in the `src/process-script/` folder. It creates and populates the directories `datalake/trusted`, `datalake/refined`, `datalake/refined_raw`, and `datalake/plots` (where the charts generated during data processing are stored).

Just like the previous container, it also uses a volume mapping, mirroring the local `../datalake` directory to `/app/datalake` inside the container. This is done using the following command:

``` sh
docker run --rm -it -v $(pwd)/../datalake:/app/datalake process-app
```

## 5. Workloads evaluated

### [WORKLOAD-1] Data Ingestion (Raw Zone)

**Description:**  

Download raw CSV datasets from the ANP open data portal, covering multiple years and semesters.

**Details:**
- Uses Python's `subprocess` to call `wget`.
- Files are saved in the `datalake/raw/` directory.
- Example:
  ```python
  download_file(2020, 1)
  ```

---

### [WORKLOAD-2] Data Preprocessing (Trusted Zone)

**Description:**  

Transforms raw CSV data into a clean and structured format for further processing.

**Steps:**
- Convert `valor_venda` and `valor_compra` from string to `double`.
- Convert `data_coleta` to `DateType`.
- Remove irrelevant columns:
  ```python
  remove_unused_cols(df)
  ```
- Add derived columns:
  ```python
  add_col_semestre(df)
  add_col_mandato(df)
  add_col_lucro(df)
  ```

---

### [WORKLOAD-3] Dimensional Modeling (Refined Zone)

**Description:**  

Creates a star schema (dimensional model) to support optimized analytical queries.

**Schema Includes:**
- `dim_endereco` – city and state.
- `dim_produto` – fuel types (e.g., GASOLINA, ETANOL, DIESEL).
- `dim_tempo` – time-based dimension with semester and mandate.
- `fato_transacao` – fact table with prices and calculated profit.

**Function used:**
```python
create_dimensional_modeling(df)
```

---

### [WORKLOAD-4] Analytical Query – Average Sale Price per Mandate

**Description:**  

Computes the average fuel sale price grouped by presidential mandate and fuel type.

**Query:**
```python
df.groupBy("mandato", "produto")   .agg(round(F.avg("valor_venda"), 2).alias("media_valor_venda"))
```

---

### [WORKLOAD-5] Time Series Aggregation – Semester Averages

**Description:**  

Calculates the average sale price and average profit per fuel type, aggregated by semester.

**Steps:**
- Group by `semestre` and `produto`.
- Compute:
  ```python
  avg(valor_venda), avg(lucro)
  ```

**Used in visualization:**
```python
plot_media_vendas_por_semestre(...)
```

---

### [WORKLOAD-6] Metrics and Storage Profiling

**Description:**  

Measures pipeline performance and data volume at each stage.

**Metrics Collected:**
- Execution time of:
  - Ingestion
  - Transformation
  - Modeling
  - Query execution
- Directory size (in MB) for each data zone:
  - `datalake/raw`
  - `datalake/trusted`
  - `datalake/refined`
  - `datalake/refined_raw`

**Function used:**

```python
get_dir_size(directory)
```

## 6. Experiments and results

### 6.1 Experimental environment

> Experiments were run on a machine with 4 vCPUs, 8GB RAM, Zorin OS (Ubuntu), Docker 24.x.

### 6.2 What did you test?

The metrics we observed were hroughput, latency and used memory.

### 6.3 Results

- First execution

[](/src/img/execucao1-primeira-populacao-da-trusted64.png)

- Second execution

[](/src/img/execucao2-repopulacao-da-trusted64.png)

- Third execution

[](/src/img/execucao3-segunda-repopulacao-da-trusted64.png)

- Fourth execution

[](/src/img/execucao4-terceira-repopulacao-da-trusted64.png)

After doing these runs we observed that thwy all had reasonably the same results.

## 7. Discussion and conclusions

### 7.1. What Worked

- Data analysis of fuel prices from 2004 to 2019.
- Performance analysis of data processing across the pipeline layers, including the impact of transformations on query speed and storage usage in each layer.

### 7.2. What Didn’t Work

- Running the project with Docker Compose, due to the need to download many files and Docker Compose's inability to wait for that download to finish.
- Creating scripts to build and execute the Docker images.

## 8. References and external resources

- [Série Histórica de Preços de Combustíveis e de GLP](https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis-e-de-glp)