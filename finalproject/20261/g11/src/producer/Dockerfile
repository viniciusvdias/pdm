# ---------------------------------------------------------------------------- #
# Producer (Kafka firehose) — imagem Docker.
# Torna a ingestao 100% Docker: nao exige Python nem venv no host.
# ---------------------------------------------------------------------------- #
FROM python:3.11-slim

WORKDIR /app

# Deps do producer (confluent-kafka, pandas, pyarrow).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# DATA_DIR e KAFKA_BOOTSTRAP sao lidos de env pelo producer.py.
# Defaults pensados para rodar DENTRO da rede do compose (listener INTERNAL).
ENV KAFKA_BOOTSTRAP=kafka:9092 \
    DATA_DIR=/data

ENTRYPOINT ["python", "producer.py"]
# Sem args por padrao => stream completo do DATA_DIR. O compose/run.sh passa
# flags como --interleave / --max-records conforme o cenario.
CMD ["--interleave"]
