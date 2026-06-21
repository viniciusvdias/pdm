FROM apache/spark:3.5.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python3 -m pip install --no-cache-dir torch==2.2.1 --index-url https://download.pytorch.org/whl/cpu --no-deps

RUN mkdir -p /tmp/ivy-cache \
    && printf 'print("warmup")\n' > /tmp/spark-package-warmup.py \
    && SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.jars.ivy=/tmp/ivy-cache \
    /tmp/spark-package-warmup.py \
    && cp /tmp/ivy-cache/jars/*.jar /opt/spark/jars/

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["/opt/spark/bin/spark-submit", "main.py"]