# Imagem Flink + PyFlink com conectores Kafka, plugin S3 (MinIO) e reporter
# Prometheus. Usada pelo JobManager, TaskManager e pelo submissor de jobs
FROM flink:1.19.1-scala_2.12-java11

#Python + PyFlink
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential; \
    ln -sf /usr/bin/python3 /usr/bin/python; \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages \
        apache-flink==1.19.1 \
        confluent-kafka==2.5.0 \
    || pip3 install --no-cache-dir \
        apache-flink==1.19.1 \
        confluent-kafka==2.5.0

#Conector Kafka no classpath do Flink
ARG KAFKA_CONN=flink-sql-connector-kafka-3.2.0-1.19.jar
RUN curl -fSL -o /opt/flink/lib/${KAFKA_CONN} \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/${KAFKA_CONN}

#Plugin S3
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY src /app/src
ENV PYTHONPATH=/app/src


COPY misc/flink/submit-jobs.sh /submit-jobs.sh
RUN chmod +x /submit-jobs.sh
