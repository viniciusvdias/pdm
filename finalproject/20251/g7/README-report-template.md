# Relatório Final do Projeto: Análise de Constrained-Off Eólico

## 1. Contexto e motivação

### 📋 Descrição

Este projeto implementa uma solução completa de big-data para análise de constrained-off (restrições de geração) em usinas eólicas brasileiras, utilizando dados consolidados do ONS (Operador Nacional do Sistema Elétrico).

### 🎯 Objetivos

1. **Processamento** dos dados consolidados via lotes.
2. **Agregações temporais e espaciais** com conversão para formato Parquet.
3. **Detecção de padrões e anomalias** nos eventos de constrained-off.
4. **Visualização interativa** das análises.
5. **Avaliação da arquitetura** proposta.

## 2. Data

### 2.1 Descrição detalhada

O dataset utilizado consiste em dois tipos complementares: **principal** e **detalhamento**.

<ins>Observação</ins>: As informações são associadas à apuração das restrições de operação por constrained-off nas usinas eólicas classificadas nas modalidades Tipo I, Tipo II-B e Tipo II-C.

Os dados são abertos e disponibilizados pela ONS, o qual pode ser acessível pelo link: https://dados.ons.org.br/.

Abaixo está representado uma descrição breve, e após, o cabeçalho de cada estrutura.

### 📊 Dados de Entrada (Consolidados)
- **Arquivo Principal**: `restricoes_coff_eolicas_consolidado.csv`
  - <ins>Definição</ins>: representa a geração, disponibilidade e restrições por usina.
  - Apresenta os dados consolidados de todos os meses com uma granularidade de 30 minutos.

- **Arquivo de Detalhamento**: `restricoes_coff_eolicas_detalhamento_consolidado.csv`
  - <ins>Definição</ins>: representa os dados de vento, geração estimada vs verificada.
  - As informações são por unidade geradora.

Os arquivos originais são históricos mensais, tal que há ~1.000.000 registros por arquivo, resultando em ~8GB de dados (considerando 42 meses de dados).

### 🧩 Cabeçalho

- Presente em ambas estruturas:
  - **id_subsistema**: Identificador do Subsistema.
  - **id_estado**: Sigla do Estado.
  - **nom_usina**: Nome da Usina ou Conjunto de Usinas.
  - **id_ons**: Identificador da Usina ou do Conjunto de Usina no ONS.
  - **ceg**: Código Único do Empreendimento de Geração (CEG), estabelecido pela ANEEL.
  - **din_instante**: Data/Hora.

- Presente somente na estrutura **principal**:
  - **nom_subsistema**: Nome do Subsistema.
  - **nom_estado**: Nome do Estado.
  - **val_geracao**: Valor da Geração, em MWmed.
  - **val_geracaolimitada**: Valor da Geração Limitada por alguma Restrição, em MWmed.
  - **val_disponibilidade**: Valor da Disponibilidade Verificada no Tempo Real, em MWmed.
  - **val_geracaoreferencia**: Valor da Geração de referência (ou estimada), em MWmed.
  - **val_geracaoreferenciafinal**: Valor da Geração de Referência Final, em MWmed.
  - **cod_razaorestricao**: Código da Razão da Restrição, podendo ser:
    - **REL** – Razão de indisponibilidade externa (elétrica).
    - **CNF** – Razão de atendimento a requisitos de confiabilidade.
    - **ENE** – Razão energética.
    - **PAR** - Restrição indicada no parecer de acesso.
  - **cod_origemrestricao**: Código da Origem da Restrição, podendo ser:
    - **LOC** – Local.
    - **SIS** – Sistêmica.

- Presente somente na estrutura **detalhamento**:
  - **nom_modalidadeoperacao**: Modalidade de Operação da Usina.
  - **nom_conjuntousina**: Nome do Conjunto de Usina (apenas para usinas com modalidade de operação Tipo II-C)
  - **val_ventoverificado**: Vento verificado, em m<sup>3</sup>/s.
  - **flg_dadoventoinvalido**: Indicativo do dado de vento inválido (quando flag = 1).
  - **val_geracaoestimada**: Valor da geração estimada da usina, em MWmed.
  - **val_geracaoverificada**: Geração verificada da usina, em MWmed.

### 🗓️ Período de Análise
  - **Início**: Outubro/2021
  - **Fim**: Abril/2025
  - **Total**: 42 meses de dados.
  - **Volume**: Dados consolidados de constrained-off eólico.

### 2.3 Como obter os dados

Uma amostra (dois arquivos complementares, totalizando um pouco mais de 512KB) está incluída no diretório `datasample/` para a execução de testes rápidos e demonstração do projeto funcionando.

Para o dataset completo, mais informações serão apresentadas posteriormente, mas basicamente pode-se obtê-lo via execução de células dos notebooks Jupyter do projeto, previamente já configuradas com o link público via Google Drive.

## 3. Como instalar e executar

> Observação: O projeto é compatível com Docker e não requer ferramentas ou instalações adicionais além do necessário.

Para uso da amostra ou do dataset completo, a inicialização é a mesma.

Para executar o projeto, clone este repositório e acesse a pasta `/g7` via terminal a fim de utilizar:

```bash
./bin/run.sh
```

Este comando irá automaticamente criar a imagem do container `jupytercli-g7`, além de realizar o deploy, via swarm, do `spark-master-g7` e do(s) `spark-worker-g7`.

Ao final da inicialização, é disponibilizado três links:
- JupyterLab:    http://localhost:8888 + (token)
- Spark Master:  http://localhost:8080
- Spark UI:      http://localhost:4040

Fica a critério do usuário acessar as referências do Spark Master e UI, porém é essencial acessar a referência do JupyterLab para a execução do projeto.

É de extrema importância que, após o usuário executar o projeto e realizar suas análises, seja efetuado o seguinte comando:

```bash
./bin/run.sh clean
```

Isso remove todos os containers, volumes, redes e o Jupyter (imagem é mantida) do projeto.

Caso seja de curiosidade do usuário, recomenda-se observar os outros comandos disponíveis pelo código:

```bash
./bin/run.sh help
```

Para o restante da execução, o usuário **DEVE** clicar na referência do JupyterLab disponível via terminal, depois acessar o notebok `src/Pre-Process.ipynb`.

### 3.1 Início rápido (usando dados de amostra)

No notebook `src/Pre-Process.ipynb`, basta seguir as instruções e executar as células `Passo 2 - Dados amostrais`.

![célula dados amostrais](images/cel_amostra.png)

### 3.2 Como executar com o dataset inteiro

No notebook `src/Pre-Process.ipynb`, basta seguir as instruções e executar as células `Passo 2 - Dataset Completo`.

![células dataset completo](images/cel_dataset.png)

### 3.3 (Opcional) Execução na VM

Durante a disciplina foi disponibilizado máquina virtuais para os grupos trabalharem seus projetos dentro delas. Portanto, para acessar a VM destinada a este grupo, pode-se executar o comando:

```bash
./bin/deploy-to-vm.sh
```

As credenciais serão solicitadas, e automaticamente o usuário será direcionado ao terminal da VM, onde já terá disponível os arquivos necessários para a execução do projeto conforme os passos deste tópico.

## 4. Arquitetura do projeto

O projeto utiliza uma arquitetura baseada no pré-processamento utilizando Apache Spark, convertendo os resultados para a extensão Parquet, a fim de se realizar as análises de constrained-off via SparkSQL.

![diagrama arquitetura do projeto](images/diagrama.jpg)

```
Consolidated CSV Files → Parquet Processing → Temporal/Spatial Aggregations → Anomaly Detection → Visualization
```

### Componentes Principais

A. **Pré-Processamento** (`Pre-Process.ipynb`)
   - Carregamento de dados consolidados.
   - Limpeza e normalização.
   - Conversão para Parquet.

B. **Comparação de Desempenho** (`Performance-Benchmark.ipynb`)
  - Compara o tempo de execução entre CSV e Parquet.
  - Analisa o uso de memória e throughput.
  - Executa as queries de detecção de anomalias em ambos os formatos.
  - Gera os relatórios de performance detalhados com recomendações.

### Fluxo de dados

1. **Ingestão**: Carregamento dos dados das usinas (A).
2. **Pré-processamento**: Limpeza e normalização (A).
3. **Análise**: Detecção de padrões constrained-off (B).
4. **Comparação**: Métricas de performance entre CSV e Parquet e derivados (B).
5. **Saída**: Relatórios de constrained-off e de performance (B).

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

## 8. Referências e recursos externos

- Apache Spark: https://spark.apache.org/
- Documentação PySpark: https://spark.apache.org/docs/latest/api/python/
- Docker: https://www.docker.com/
- Google Drive

- List all external resources, datasets, libraries, and tools you used (with links).
