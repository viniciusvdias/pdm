# 🍕 Kafka Fake Stream - Setup Guide

Este repositório permite iniciar um ambiente Docker para simular dados falsos em Kafka, com integração ao Jupyter e Spark local.

## ✅ Pré-requisitos

- Docker e Docker Compose instalados
- `make` disponível
- Ambiente Linux ou compatível com Bash

---

## ⚙️ Passos para configuração

### 1. Modificar o Makefile da rede Docker

Abra o arquivo `./pdmnet/Makefile` e altere a linha de criação da rede para:

```bash
docker swarm init --advertise-addr 127.0.0.1 || docker network create --scope=swarm --attachable -d overlay pdmnet
```

Depois disso, execute:

```bash
make
```

---

### 2. Usar o Jupyter embarcado no Docker

> ⚠️ **Importante:** **Não** use o Jupyter do VSCode. Use o Jupyter que está dentro do container Docker.

---

### 3. Ajustar o Makefile da raiz do projeto

Abra o `Makefile` na raiz do projeto e remova a linha referente ao `buildvmacces` (se existir).
Depois execute:

```bash
make
```

Quando o processo terminar, inicie o Jupyter com:

```bash
./jupytercli/bin/jupytercli-start.sh
```

---

### 4. Iniciar o Kafka via Docker Swarm

Com o Docker Swarm inicializado e a rede `pdmnet` configurada, execute:

```bash
./kafka/bin/kafka-start-docker-swarm.sh
```

---

### 5. Gerar dados falsos para o tópico `pizza`

Execute o comando abaixo para publicar dados no Kafka:

```bash
host=kafka port=9092 topic=pizza nmessages=10 maxwaitingtime=5 subject=pizza ./kafkafakestream/bin/kafkafakestream-start.sh
```

---

### 6. Ajuste no Jupyter Notebook

Dentro do notebook, ao iniciar o Spark, **troque** o endereço:

```python
"spark://spark:7077"
```

por:

```python
"local[*]"
```

Assim, o Spark será executado localmente.

---

## ✅ Observações Finais

- Certifique-se de que os containers estão rodando corretamente.
- Verifique se a rede `pdmnet` foi criada com escopo `swarm` e é `attachable`.
- Use sempre o terminal para iniciar os scripts.
