# Relatório Final do Projeto: *Processamento de Dados Musicais do Spotify com PySpark*
## 1. Contexto e motivação

Nos últimos anos, com o crescimento de plataformas de streaming como o Spotify, uma enorme quantidade de dados musicais passou a ser gerada, incluindo letras, metadados, métricas de áudio e classificações emocionais. Esse volume de dados abre oportunidades para análises em larga escala sobre padrões linguísticos, sentimentos e relações entre linguagem e música.

O objetivo principal deste projeto é analisar e processar letras de músicas em escala utilizando ferramentas de Big Data, em particular Apache Spark, com foco em:

- Identificar as palavras mais associadas a diferentes emoções musicais

- Avaliar a distribuição e frequência dessas palavras em um corpus massivo

- Otimizar o processamento através de formato Parquet e execução distribuída

O projeto utiliza um dataset real com aproximadamente 500 mil músicas (extraído do Spotify), com atributos como gênero, artista, letra e emoção associada, o que seria inviável de processar eficientemente com ferramentas tradicionais.

Além disso, o trabalho propõe uma infraestrutura distribuída replicável via Docker Swarm, simulando um cluster Spark com múltiplos nós, de forma prática e portátil, permitindo testar desempenho, escalabilidade e custo computacional em diferentes configurações.

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

Para baixar e extrair os dados do projeto entre no site do kaggle https://www.kaggle.com/datasets/devdope/900k-spotify e faça o download do zip do dataset, ou siga os passos abaixo:

1. Baixe o arquivo zip do dataset usando a API do Kaggle:
 ```bash
curl -L -o 500k-spotify.zip \
https://www.kaggle.com/api/v1/datasets/download/devdope/900k-spotify
```
2. Extraia o conteúdo do arquivo zip:
```bash
unzip 500k-spotify.zip
```
3. Após a extração, um dos arquivos disponíveis será `spotify_dataset.csv` que foi usado no presente projeto

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

### 3.1 Quick start (usando uma amostra de dados em `datasample/`)
1. Faça uma copia do dataset de exemplo `spotify_dataset.csv` presente do diretório `/datasample` ao diretório `src/spark-data`
2. Acesse `http://localhost:8888/lab?token=spark123`
3. Faça o pré-processamento dos dados executando os notebooks:
   - csv_to_parquet.ipynb
   - tratamento_full_dataset.ipynb 

4. Para rodar os workloads, execute os notebooks:
   - workload_1.ipynb
   - workload_2.ipynb
   - workload_3.ipynb

### 3.2 Como rodar com todo o dataset

1. Utilize o csv baixado no passo 2.2 e coloque esse arquivo `spotify_dataset.csv` no diretório `src/spark-data`
2. Acesse `http://localhost:8888/lab?token=spark123`
3. Faça o pré-processamento dos dados executando os notebooks:
   - csv_to_parquet.ipynb
   - tratamento_full_dataset.ipynb 

4. Para rodar os workloads, execute os notebooks:
   - workload_1.ipynb
   - workload_2.ipynb
   - workload_3.ipynb


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

#### [WORKLOAD-1] Agrupamento de letras de músicas por emoção

**Objetivo:** Mostrar quais palavras são mais frequentes nas letras das músicas em cada emoção.

**Etapas:**
- Leitura do dataset no formato Parquet.
- Remoção de pontuação e símbolos usando `regex_replace`.
- Remoção de StopWords (palavras sem sentido semântico).
- Tokenização por espaços.
- Conversão para minúsculas.
- Explosão das palavras em linhas individuais usando `explode`.
- Ranqueamento de palavras mais recorrentes por emoção.

---

#### [WORKLOAD-2] Cálculo da Similaridade de Jaccard

**Objetivo:** Comparar todos os pares possíveis de artistas dentro do mesmo gênero utilizando a métrica de Jaccard com base no vocabulário textual.

**Etapas:**
- Realização de um self-join (cruzamento) entre os artistas por gênero.
- Cálculo de interseção (`array_intersect`) e união (`array_union`) de vocabulários.
- Cálculo da similaridade de Jaccard: `interseção / união`.
- Ranqueamento dos pares mais similares por gênero.

---

#### [WORKLOAD-3] Similaridade Léxica entre Gêneros Musicais

**Objetivo:** Encontrar o número de palavras em comum entre os principais gêneros musicais, medindo similaridade léxica.

**Etapas**:

- Leitura do dataset `musicas_limpas.parquet`.
- Seleção dos 10 gêneros musicais com mais músicas no dataset.
- Explosão das palavras com `explode` e `split`.
- Agrupamento por `main_genre` usando `collect_set("palavra")` para obter vocabulário único por gênero.
- Self-join do vocabulário de gêneros usando `crossJoin`, comparando pares distintos (`g1 < g2`).
- Cálculo do número de palavras em comum por par de gêneros usando `array_intersect` + `size`.
- Ordenação decrescente pelo número de palavras em comum.

---

- Specify each big data task evaluated in your project (queries, data pre-processing, sub-routines, etc.).
- Be specific: describe the details of each workload, and give each a clear name. These named workloads will be referenced and evaluated via performance experiments in the next section.
  - Example: [WORKLOAD-1] Query that computes the average occupation within each
    time window (include query below). [WORKLOAD-2] Preprocessing, including
  removing duplicates, standardization, etc.

---

## 6. Experiments and results

### 6.1 Experimental environment

- As execuções foram realizadas em ambiente Docker com Spark.
- Os experimentos foram realizados em uma máquina:
> Windows 11
> Ubuntu 22.04 (containers)
> Docker Version 28.3.0
> Spark Version 3.4.1
> Modo Cluster
> 1 Spark Master + N workers (varíavel)
> CPU 3.70 Ghz base (até 4.6 Ghz)
> 16 GB de RAM DDR4 3200 MHz

### 6.2 What did you test?

Foram avaliadas as seguintes variações de configuração:

| Parâmetro                 | Valores Testados              |
|--------------------------|-------------------------------|
| Workers                  | 1, 2, 6                        |
| Núcleos por Worker       | 1, 2, 6                        |
| Métricas observadas      | Tempo total (s), uso de memória, distribuição de tarefas, uso de CPU, Throughput (MB/s) |
| Repetições               | 3 execuções por configuração   |

### 6.3 Results
#### Tabela Comparativa por Configuração
 Todas as execuções foram feitas no mesmo arquivo parquet de 424 MB

### Workload 1 - Agrupamento de letras de músicas por emoção

| WORKERS | CORES | TEMPO (s) | Throughput (MB/s) |
|---------|-------|-----------|------------|
| 1       | 1     | 163       | 2.59       |
| 2       | 1     | 106       | 3.99       |
| 4       | 1     | 74        | 5.72       |
| 6       | 1     | 63        | 6.74       |
| 8       | 1     | 61        | 6.90       |
| 16      | 1     | 53        | 7.82       |


---

### Workload 2 - Cálculo da Similaridade de Jaccard

| WORKERS | CORES | TEMPO (s) | Throughput (MB/s) |
|---------|-------|-----------|------------|
| 1       | 1     | 91        | 4.66       |
| 1       | 6     | 49        | 8.65       |
| 2       | 2     | 70        | 6.06       |
| 6       | 1     | 59        | 7.19       |
| 6       | 6     | 46        | 9.22       |

---

### Workload 3 - Similaridade Léxica entre Gêneros Musicais

| WORKERS | CORES | TEMPO (s) | Throughput (MB/s) |
|---------|-------|-----------|------------|
| 1       | 1     | 269       | 1.58       |
| 1       | 6     | 122       | 3.48       |
| 2       | 2     | 116       | 4.66       |
| 6       | 1     | 163       | 2.60       |
| 6       | 6     | 105       | 4.04       |

---

## 7. Discussion and Conclusions

### O que funcionou bem:

- A arquitetura com **Apache Spark em Docker Swarm** se mostrou eficiente e escalável para os workloads propostos.
- Os workloads principais conseguiram demonstrar claramente os ganhos de paralelismo ao aumentar o número de workers e núcleos.
- As operações mais custosas computacionalmente (como `crossJoin` e `array_intersect`) foram bem distribuídas no cluster com a configuração adequada de particionamento (`spark.sql.shuffle.partitions`).
- A utilização do método `cache()` em pontos estratégicos (como o vocabulário pré-computado) evitou recomputações dispendiosas e otimizou o tempo final.
- O workload intermediário que calcula interseções entre gêneros diferentes (sem filtragem por gênero) também forçou um bom estresse no cluster, atingindo milhares de combinações.
- As análises mostraram que a distribuição de tarefas foi mais eficiente em configurações com múltiplos workers e múltiplos núcleos (como 6 workers × 6 cores).

### O que não funcionou ou teve limitações:

- Workloads aparentemente pesados tiveram performance surpreendentemente leve. Isso se deve ao pequeno número de combinações possíveis, tornando a operação leve para o cluster.
- Inicialmente, não havia particionamento adequado definido em algumas operações com janela (`Window`), o que gerava **`WARN WindowExec: No Partition Defined`** e resultava em reagrupamento de todos os dados em uma única partição, prejudicando a paralelização.
- A performance foi limitada em alguns testes pelo uso de limites de recursos (CPU/memória) definidos no `docker-compose.yml`, o que afetou muito o desempenho.
- O `repartition()` só surtiu efeito visível em workloads intensos e em joins com grandes cardinalidades.

### Desafios encontrados:

- Garantir o balanceamento do workload entre os workers, visto que a simples definição de partições nem sempre resultava em uso igualitário dos recursos.
- Entender e validar visualmente (via UI do Spark) se os workers estavam realmente processando dados e não apenas o driver.
- Reproduzir experimentos de forma consistente exigiu controle rigoroso do ambiente (versão do Docker, reinício dos containers, cache clean).

## 8. References and External Resources

### Bibliotecas e Frameworks

- [Apache Spark](https://spark.apache.org/) — processamento distribuído de dados.
- [PySpark API](https://spark.apache.org/docs/latest/api/python/) — interface Python para Apache Spark.
- [Docker](https://www.docker.com/) — conteinerização do ambiente.
- [Docker Compose](https://docs.docker.com/compose/) — orquestração local de containers.
- [Spark UI](https://spark.apache.org/docs/latest/web-ui.html) — ferramenta visual para monitoramento.

### Dataset

- Dataset Utilizado - [Spotify Dataset](https://www.kaggle.com/datasets/devdope/900k-spotify?select=final_milliondataset_BERT_500K_revised.json)
- Total de registros: milhares de músicas com metadados como artista, gênero, letra e emoções.
- Transformado em `.parquet` para leitura otimizada no cluster.

### Inspirações e fontes de consulta

- [Spark Optimization Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- Stack Overflow e fóruns sobre uso de `array_intersect`, joins cruzados e tuning de `Window` no Spark.
- Documentações de `repartition`, `cache`, `join hints` e `crossJoin` no contexto do PySpark.


*Esse relatório foi desenvolvido com base em testes práticos, medições diretas via tempo de execução e inspeção da interface web do Spark em ambiente distribuído via Docker Swarm.*

---