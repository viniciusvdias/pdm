FROM apache/spark:3.5.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3.9 python3.9-dev python3-pip \
    && python3.9 -m pip install --no-cache-dir --upgrade pip \
    && rm -rf /var/lib/apt/lists/*

# Faz o Spark (driver e workers Python) usar o 3.9
ENV PYSPARK_PYTHON=python3.9
ENV PYSPARK_DRIVER_PYTHON=python3.9

WORKDIR /app

# Build do PyTorch com CUDA 12.8 — necessário para GPUs Blackwell (RTX 50xx / sm_120).
RUN python3.9 -m pip install --no-cache-dir torch==2.7.0 --index-url https://download.pytorch.org/whl/cu128

RUN mkdir -p /tmp/ivy-cache \
    && printf 'print("warmup")\n' > /tmp/spark-package-warmup.py \
    && SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.jars.ivy=/tmp/ivy-cache \
    /tmp/spark-package-warmup.py \
    && cp /tmp/ivy-cache/jars/*.jar /opt/spark/jars/

COPY requirements.txt .
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["/opt/spark/bin/spark-submit", "main.py"]
