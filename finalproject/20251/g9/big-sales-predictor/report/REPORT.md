# Big Sales Predictor

Projeto de análise preditiva de vendas com Big Data, usando Apache Spark, PySpark, Spark MLlib e Docker Swarm.

## Objetivo

Construir um pipeline de machine learning distribuído para prever o valor de vendas com base em dados históricos.

---

## Pré-requisitos

- Docker instalado
- Docker Swarm ativado
- Dataset: [Big Sales Data – Kaggle](https://www.kaggle.com/datasets/pigment/big-sales-data)

---

## Como Executar o Projeto

### 1. Adicione o arquivo CSV

Baixe o arquivo `big_sales_data.csv` do Kaggle e mova para a pasta `/data`:

```bash
mv ~/Downloads/big_sales_data.csv ./data/
```

---

## Etapas com Docker Swarm

### 2. Inicie o Docker Swarm

```bash
docker swarm init --advertise-addr 192.168.15.10
```

---

### 3. Construa a imagem da aplicação

```bash
docker build -t big-sales-app .
```

---

### 4. Faça o deploy da stack

```bash
docker stack deploy -c docker-compose.yml big-sales
```

---

### 5. Verifique os serviços

```bash
docker service ls
```

---

## Execução Manual (script por script)

Se quiser rodar os scripts individualmente (útil para testes):

> **Pré-requisito:** Spark instalado localmente ou PySpark no ambiente Python

### 1. Pré-processamento
```bash
python3 src/preprocessing.py
```

### 2. Treinamento do Modelo
```bash
python3 src/train_model.py
```

### 3. Predição com o modelo treinado
```bash
python3 src/predict.py
```
---
