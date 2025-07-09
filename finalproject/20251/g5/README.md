# Relatório Final do Projeto: *Processamento de Dados Musicais do Spotify com PySpark*
## 1. Contexto e motivação

- What is the main goal? What problem are you trying to solve with big data?

## 2. Dados 🎧

Este projeto utiliza o dataset “500K+ Spotify Songs with Lyrics,Emotions & More”, disponível publicamente no Kaggle:

🔗 Fonte oficial: https://www.kaggle.com/datasets/devdope/900k-spotify

### 2.1 **O que o dataset contém**:
- Aproximadamente 500 mil registros de músicas
- Cada linha representa uma música e seus dados
- Tamanho aproximado: 1,3 GB
- Está disponível nos formatos .csv e .json
- São 39 colunas com atributos como:
    - Nome da música, artista, álbum, gênero, data de lançamento
    - Letra completa da música
    - Emoção da música (campo emotion) associada à letra
    - Características Técnicas e popularidade da música
    - Indicadores de Atmosfera Musical (energia, positividade, dança, acústica, instrumentalidade etc.)
    - Usos Sugeridos para a Faixa (Good for Party, Good for Work, etc)
    - Indicações por Similaridade Sonora

### 2.2 Como obter os dados


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
### ⚙️ **Inicialização do ambiente com Docker Swarm**

1. Acesse a pasta `src` do projeto para rodar os comandos `pdmtf`
```bash
cd src
```
2. Inicie os serviços Spark com Jupyter usando Docker Swarm, esse comando sobe os serviços: Jupyter Notebook, Spark Master e Spark Worker
```bash
pdmtf init
```
3. Execute um `docker ps` para verificar se o containers estão ativos e acesse a interface web em `http://localhost:8888/lab?token=spark123`

4. Execute `pdmtf help` para visualizar outros comandos disponíveis como `pdmtf stop` (Parar o cluster Spark) ou `pdmtf scale 3` (Alterar o número de workers)


## 4. Arquitetura do projeto

O projeto utiliza contêineres orquestrados com Docker Swarm para processar dados com Apache Spark e interagir via Jupyter Notebook. Seus principais componentes são:

- **Usuário (navegador web):** Acessa a interface do Jupyter Notebook em `http://localhost:8888` para escrever e executar scripts PySpark. O navegador não integra o cluster, mas é o ponto de entrada para o usuário.

- **Jupyter Notebook (Spark Driver):** Executado em um contêiner, fornece a interface web e atua como Spark Driver. Inicia a SparkSession e envia os jobs para o cluster via `spark://spark-master:7077`. Compartilha o volume /spark-data com os demais serviços.

- **Spark Master:** Coordena o cluster, recebendo jobs do Driver e distribuindo-os entre os Workers. Roda em contêiner próprio e expõe sua interface de monitoramento em `http://localhost:8080`.

- **Spark Workers:** Executam as tarefas de forma distribuída. Conectam-se automaticamente ao Master e acessam o volume /spark-data para leitura e escrita. São contêineres escaláveis no Docker Swarm.

- **Volume /spark-data:** Pasta compartilhada entre todos os contêineres, usada para armazenar dados de entrada (.csv) e saída (.parquet). Elimina a necessidade de transferência via rede.

- **Rede spark-net:** Interliga todos os contêineres, permitindo comunicação entre os serviços.

<p align="center">
  <img src="presentation/arquitetura-big.png" alt="Texto alternativo" width="700"/>
</p>

- Fluxo de Dados:

  ```
  [CSV] → [Transformação Parquet] → [Limpeza] → [Processamento] → [Resultados]
  ```



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