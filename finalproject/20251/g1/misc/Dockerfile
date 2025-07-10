FROM bitnami/spark:latest

USER root

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV HADOOP_USER_NAME=root

# Instala dependências do sistema e Python
RUN apt-get update && \
    apt-get install -y curl unzip python3-pip netcat-openbsd wget && \
    pip install --upgrade pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instala o Hadoop client manualmente (para o comando hdfs funcionar)
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /opt/hadoop && \
    ln -s /opt/hadoop/bin/hdfs /usr/local/bin/hdfs && \
    rm hadoop-3.3.6.tar.gz

# Instala dependências Python
COPY data/enem_data/2020/DADOS/MICRODADOS_ENEM_2020.csv /data/enem_data/2020/DADOS/MICRODADOS_ENEM_2020.csv
COPY data/enem_data/2021/DADOS/MICRODADOS_ENEM_2021.csv /data/enem_data/2021/DADOS/MICRODADOS_ENEM_2021.csv
COPY data/enem_data/2023/DADOS/MICRODADOS_ENEM_2023.csv /data/enem_data/2023/DADOS/MICRODADOS_ENEM_2023.csv
RUN pip install requests

# Copia o job principal
COPY src/main.py /opt/spark/jobs/main.py

WORKDIR /opt/spark/jobs

# Aplica as configurações sysctl no container e inicia o bash
CMD ["bash", "-c", "sysctl -p && /bin/bash"]