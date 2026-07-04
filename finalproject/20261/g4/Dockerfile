# Imagem única para o coletor e o processador Spark.
# O PySpark roda em modo local dentro do container, então precisa da JVM.
FROM python:3.12-slim

# Java (para o PySpark) + procps (o Spark usa o comando `ps`)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Comando default; cada serviço no docker-compose sobrescreve com o seu.
CMD ["python", "-u", "binance.py"]
