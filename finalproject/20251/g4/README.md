# Final project report: *Análise de dados metereológicos brasileiros*

## 1. Context and motivation

- O principal objetivo é prover análise interessantes a respeito de dados metereológicos do Brasil. Essas análises podem ser utilizadas como base para tomada de decisões no futuro.
- O foco maior é conseguir gerenciar toda essa quantidade de dados históricos, sendo capaz de resumir todos esses dados em tabelas, gráficos e modelos preditivos. 
- Motivação é identificar padrões nos dados para precipitação e temperatura em regiões do Brasil.

## 2. Data

### 2.1 Detailed description

- O dataset utilizado é denominado: Climate Weather Surface of Brazil.
  - Os dados foram originalmente encontrados na plataforma Kaggle, através desse link: https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region,
  porém, a fonte oficial é o INMET (National Meteorological Institute - Brazil), como descrito na descrição do dataset na página do Kaggle.
  - O dataset basicamente contém dados de 623 estações metereológicas do Brasil. Ao todo, temos 27 colunas (6 categóricas e 21 numéricas) e 61710480 instâncias. 

### 2.2 How to obtain the data

- Para baixar o dataset completo, é necessário que primeiramente execute a seção 3 para que o projeto seja inicializado. Com o projeto inicializado, no terminal do jupyter lab, execute o script "getData.sh" da seguinte forma. 
- Se preferir, no arquivo "misc/get_dataset.ipynb" pode-se executar a célula para que o script seja executado.

  ```bash
  chmod +x ./bin/getData.sh
  ./getData.sh
  ```


## 3. How to install and run

### 3.1 Quick start (using sample data in `datasample/`)

- Para executar o projeto, basta rodar o seguinte comando:

  ```bash
  ./bin/deploy.sh
  ```

- Com isso, acesse o link fornecido http://localhost:8888, para entrar no jupyterlab.
- Caso não encontre a página, espere alguns segundos e recarregue a página.
- Quando a página carregar, aparecerá uma página que requisita um token para continuar. No campo específico, coloque a palavra "token" para entrar no jupyter lab.
- A seguir, rode todas as células do sample_regression.ipynb para realizar uma amostra da regressão e do sample_analysis.ipynb para realizar uma amostra da análise.
- Todos os notebooks estão na pasta src

### 3.2 How to run with the full dataset

- Para rodar no dataset inteiro, basta rodar o script que prepara o dataset inteiro, de qualquer uma das formas citadas acima e rodar o notebook desejado para regressão (regression.ipynb) ou análise dos dados (sample_analysis.ipynb)
- Todos os notebooks estão na pasta src

## 4. Project architecture

- Esse é o esquema principal da arquitetura do projeto.

![alt text](misc\arquitetura.png)

1. Leitura distribuída dos dados
- Arquivos CSV que serão baixados, extraídos e lidos, cada um de uma região específica.
1. Merge dos CSV em apenas 1 único CSV, contendo todos os dados.
2. Pré-processamento
- Limpeza e tratamento para remoção de duplicatas, tratar de valores não plausíves(ex: -9999)
1. Visualização única
- Plot de gráficos a partir de agregações usando o dask e o plotly.express da px, resultando em:
  - Precipitação por região;
  - Temperatura e precipitação por estado;
  - Temperatura anual por região;
  - Temperatura e precipitação por estação do ano.
1. Predição de temperatura:
  - Foi utilizado modelo preditivo de regressão, utilizando as ferramentas daskML, dask DataFrame e LGBM.
2. Execução em containers
  - Utilizou-se docker para isolar componentes em serviços, sendo eles: dask-scheduler, dask-worker e jupyter.
  - Comunicação por meio da rede interna dask-net.
3. Interface de monitoramento
  - JupyterLab: http://localhost:8888


## 5. Workloads evaluated

- O que buscamos foi a análise dos dados através da geração de gráficos e a criação de um modelo de regressão.
- *Regressão:* 
  - **Objetivo:** Criação de um modelo de regressão para predizer o valor da coluna: “TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)”, utilizando os outros atributos presentes na base de dados.
  - **Metodologia:** Utilizou-se Dask, DaskML, DaskDataframe e a biblioteca LightGBM. 
  - **Leitura e Limpeza(Dask e DaskDataframe):** Feita com Dask e DaskDataframe, fizemos a leitura dos dados, remoção de colunas relacionadas a outras medidas de temperatura, filtragem de valores indesejados, codificação de valores categóricos para valores numéricos.
  - **Treinamento(DaskML e LightGBM):** Inicialmente, foi realizado uma divisão dos dados, em treino e teste com a intenção de treinar o modelo na primeira e avalia-lo, usando a segunda. 
  - **Escolha do modelo:** Por conta do tamanho do dataset, optamos por um modelo mais robusto, que fosse capaz de identificar com maior facilidade os padrões no grande número de dados.
  - **Avaliação do modelo:** Feita com a métrica erro médio absoluto, que fornece um valor facilmente interpretável.
- *Análise dos gráficos:*
  - Os gráficos foram gerados através da biblioteca Plotly 
    - Temperatura Média Anual por Região
    - Precipitação Total Anual por Região
    - Precipitação Total por Estado (todos os anos)
    - Médias de Temperaturas por Estado (todos os anos)
    - Precipitação Total por Estação do Ano (todos os anos)
    - Médias de Temperaturas por Estação do Ano (todos os anos)
  - Valores indesejados (-9999) foram removidos.
  - Novas as colunas foram adicionadas: Ano, Mês, Estação do ano.


## 6. Experiments and results

### 6.1 Experimental environment

> Experimentos da **Regressão** foram feitos em uma máquina com um processador *13th Gen Intel(R) Core(TM) i7-13700F* e *64GB RAM*, *Ubuntu 22.04*, *Docker 24.x*.

> Experimentos da **Análise dos gráficos** foram feitos em uma máquina com um processador *10th Gen Intel(R) Core(TM) i5-10400F* e *16GB RAM*, *Ubuntu 22.04*, *Docker 24.x*.

### 6.2 What did you test?

- Testamos o tempo de execução do treinamento e da predição do modelo de regressão. 
  - Usando 80% do dataset, o treinamento levou 23 minutos e 52 segundos.
  - Usando 20% do dataset, a predição levou 442 milisegundos.

### 6.3 Results

Os gráficos a seguir foram gerados a partir de agregações aplicadas sobre dados climáticos brasileiros utilizando a biblioteca Plotly. A análise abrange o período entre os anos 2000 e 2021, considerando informações de temperatura máxima, temperatura mínima e precipitação total, agrupadas por região, estado e estação do ano.

**Temperatura Média Anual por Região**
![alt text](misc\temperaturaMedia.png)
Em 2021, algumas regiões atingiram suas maiores médias de temperatura desde o início da série histórica.

As regiões Norte e Nordeste se destacam como as mais quentes, mantendo médias mais elevadas ao longo dos anos.

A região Sul, por outro lado, apresenta as menores temperaturas médias consistentemente no período.

A análise permite observar variações temporais dentro de cada região e comparar o comportamento térmico entre elas.

**Precipitação Total Anual por Região**
![alt text](misc\precipitacaoTotal.png)
Os dados indicam um aumento na cobertura das estações meteorológicas ao longo dos anos, especialmente a partir de 2008. Isso é evidenciado pelo aumento nos volumes de precipitação registrados, que anteriormente apareciam subestimados.

O ano de 2009 foi atípico no Nordeste, com registro de chuvas acima da média — o que é consistente com reportagens da época sobre alagamentos e eventos climáticos extremos.

Em contrapartida, 2021 apresentou baixos índices de precipitação em todas as regiões, compatível com registros de seca histórica no Brasil.

**Precipitação Total por Estado (Todos os Anos)**
![alt text](misc\mediaPrecipitacaoTotal.png)
O estado de Minas Gerais (MG) apresenta o maior volume acumulado de precipitação no conjunto de dados.

Essa alta pode refletir tanto a realidade pluviométrica da região quanto uma concentração maior de estações meteorológicas em determinadas áreas.

Estados da região Norte, como RR e AP, apresentaram volumes mais baixos, possivelmente devido à menor cobertura de estações nessas áreas.

**Médias de Temperaturas por Estado (Todos os Anos)**
![alt text](misc\tempMimMaxPrecipitacao.png)
O estado do Piauí (PI) foi identificado como o estado com a maior média de temperatura máxima ao longo do período analisado.

Por outro lado, o estado de Santa Catarina (SC) teve a menor média de temperatura máxima, reforçando o padrão climático mais ameno do sul do país.

Há um gradiente térmico claro entre o norte/nordeste (mais quente) e o sul (mais frio).

**Precipitação Total por Estação do Ano (Todos os Anos)**
![alt text](misc\todosAnosPrecipitacaoTotal.png)
Os dados confirmam o padrão climático brasileiro, com chuvas concentradas no verão e uma redução acentuada no inverno.

O verão apresentou o maior volume de precipitação, reforçando a importância dessa estação no regime hídrico nacional.

**Médias de Temperaturas por Estação do Ano (Todos os Anos)**
![alt text](misc\todosAnosTempMaxMin.png)
A média das temperaturas máximas e mínimas segue o comportamento esperado:

Verão → temperaturas mais elevadas.

Inverno → temperaturas mais baixas.

A transição das médias térmicas pelas estações evidencia a influência da sazonalidade no clima brasileiro.

## 7. Discussion and conclusions

- **What worked:**

  - Durante o desenvolvimento do projeto, conseguimos realizar uma análise exploratória eficaz dos dados climáticos brasileiros, contemplando variáveis como temperatura máxima, temperatura mínima e precipitação. A visualização dos dados por meio de gráficos facilitou a compreensão dos padrões climáticos regionais ao longo do tempo.
  - Além disso, implementamos e treinamos com sucesso um modelo de regressão linear utilizando diversos atributos climáticos, com o objetivo de prever a temperatura.
  - Um destaque importante foi a utilização da biblioteca Dask, que nos permitiu processar grandes volumes de dados de forma eficiente, aproveitando o paralelismo e a distribuição de tarefas.

- **What did not work:**
  - Apesar dos avanços obtidos, enfrentamos dificuldades na configuração dos workers do Dask via Docker. Esse obstáculo limitou a escalabilidade do nosso ambiente de execução distribuída e nos impediu de explorar todo o potencial do Dask em ambientes multi-nós.

- Tivemos dificuldades em integrar o docker com o dask. Isso porque estavamos enfrentando um problema que os workers "morriam" repentinamente sem um aparente motivo. Por conta disso, tornou-se inviável acessar o dask dashboard no ambiente docker.

## 8. References and external resources

- https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region
- https://www.dask.org/
- https://lightgbm.readthedocs.io/en/stable/
- https://www.docker.com/
- https://matplotlib.org/stable/tutorials/pyplot.html
