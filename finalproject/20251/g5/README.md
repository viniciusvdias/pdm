# Final project report: *\<include your title here\>*
# COMO RODAR O PROJETO 

- Clonar esse repositorio
`git clone https://github.com/PedroCobucci/pdm.git`


- Checkout para a branch do grupo 
`git checkout finalproject-20251-G5`


- Acessar a pasta do projeto do grupo e pasta do script 
`cd finalproject/20251/g5/bin`

```bash
./bin/pdmtf setup
source ~/.bashrc
pdmtf
```

- Para que os comandos `pdmtf` funcionem, acesse a pasta src
`cd src`

```bash
pdmtf init
```


# Template Project Structure (`gX`)

## 1. Context and motivation

- What is the main goal? What problem are you trying to solve with big data?

## 2. Data

### 2.1 Detailed description

- Describe the dataset(s) used in your project.
  - Where does the data come from? Include the source/link.
  - What does the data contain? (number of records, type of features, etc.)
  - If data is generated, explain how and include instructions to generate a sample with Docker.

### 2.2 How to obtain the data

- A small sample dataset (at most 1MB, preferably less) must be included in the `datasample/` folder of this repository. This sample is required so the project can be tested quickly.
- For the full dataset only (not the sample), provide clear instructions on how to download or generate it. Do not include the full dataset in the repository.
- For example, you can provide public links to the data and commands on how to download them using tools like `wget` (this can also be wrapped in a Docker container):

  ```bash
  wget https://path-to-your-public-dataset/data.zip
  unzip data.zip -d data/
  ```

- If the dataset is hosted on cloud storage (Google Drive, HTTPS, AWS S3, etc.), provide the public link and the necessary commands.
- If the data must be generated, provide the Docker command to generate it.

## 3. Como rodar o projeto

1. Clone o repositório do projeto
2. Acesse a branch do grupo e a a pasta do projeto do grupo
```bash
git checkout finalproject-20251-G5

cd finalproject/20251/g5
```
3. Execute o script de configuração inicial e ative as configurações no terminal
```bash
./bin/pdmtf setup
source ~/.bashrc
pdmtf
```
### ⚙️ Inicialização do ambiente com Docker Swarm

1. **Acesse a pasta `src` do projeto para rodar os comandos `pdmtf`**
```bash
cd src
```
2. Inicie os serviços Spark com Jupyter usando Docker Swarm
```bash
pdmtf init
```

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

## 4. Arquitetura do projeto

O projeto utiliza uma arquitetura em contêineres orquestrada com **Docker Swarm**, focada em processar dados com **Apache Spark** e interagir via **Jupyter Notebook**. Os principais componentes são:

- **Usuário (navegador web): O usuário** interage com o sistema através do navegador web, acessando a interface do Jupyter Notebook em `http://localhost:8888`. Por meio dessa interface, ela escreve e executa códigos PySpark para processar e analisar dados. O navegador em si não faz parte do cluster, mas é o ponto de entrada do sistema para o usuário.
- **Jupyter Notebook (Spark Driver):** O Jupyter Notebook roda dentro de um contêiner Docker e serve como o ambiente interativo para desenvolvimento e execução de scripts PySpark. Além de prover a interface web  ele também atua como o **Spark Driver**, responsável por iniciar a sessão Spark (`SparkSession`) e enviar os jobs para o cluster. O notebook se comunica diretamente com o Spark Master usando o endereço `spark://spark-master:7077`, e também compartilha com os demais serviços o volume `/spark-data`, onde estão os arquivos CSV de entrada e os arquivos Parquet gerados como saída.
- **Spark Master:** O Spark Master é o componente central de coordenação do cluster Spark. Ele recebe as tarefas submetidas pelo Driver (no caso, o Jupyter) e distribui essas tarefas entre os Spark Workers disponíveis. Ele roda em um contêiner separado dentro do Docker Swarm e expõe sua interface de monitoramento em `http://localhost:8080`. Por meio dessa interface, é possível visualizar os jobs em execução, os Workers ativos e o status geral do cluster.
- **Spark Workers:** Os Spark Workers são os nós executores do cluster. São responsáveis por processar os dados de forma distribuída, executando as transformações e ações definidas nos scripts PySpark. Eles se registram automaticamente no Spark Master e recebem tarefas conforme a necessidade de execução. Cada Worker possui acesso ao volume `/spark-data`, permitindo que leiam arquivos de entrada e escrevam os resultados localmente no volume compartilhado. Os Workers também são contêineres Docker, podendo ser escalados horizontalmente dentro do Docker Swarm para simular ambientes maiores.
- **Volume compartilhado `/spark-data` :** O volume `/spark-data` é montado a partir de uma pasta do host e é compartilhado entre todos os contêineres do projeto: Jupyter, Spark Master e Spark Workers. Esse volume é usado para persistência de dados e comunicação entre os componentes. Nele, são armazenados os arquivos de entrada (como arquivos `.csv`) e os arquivos gerados pelos jobs Spark (como `.parquet`). A existência desse volume evita a necessidade de transferência de dados entre containers via rede, simplificando o fluxo de dados e facilitando o uso com ferramentas locais.
- **Rede Docker `spark-net`:** Todos os serviços são conectados por meio de uma rede chamada `spark-net`. Essa rede permite que os contêineres se comuniquem entre si.

<p align="center">
  <img src="presentation/arquitetura-big.png" alt="Texto alternativo" width="700"/>
</p>

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