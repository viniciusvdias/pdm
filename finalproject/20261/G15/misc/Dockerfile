# Imagem NGC da NVIDIA: TensorFlow otimizado com CUDA 12.8 (suporte a Blackwell / RTX 50).
# Ultima release TF da NVIDIA (25.02) que roda na RTX 5060 (sm_120).
FROM nvcr.io/nvidia/tensorflow:25.02-tf2-py3

WORKDIR /tf

# Java 17 para o Spark 3.5.0 (suporta Java 8/11/17). O default-jre da imagem (Ubuntu 24.04)
# puxava Java 21, incompativel com o Arrow do PySpark
# (erro: "sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available").
RUN apt-get update && apt-get install -y openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# PySpark + ferramentas de monitoramento. NAO instalar tensorflow aqui:
# usamos o TF otimizado (Blackwell/CUDA 12.8) que ja vem na imagem NGC.
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# A imagem NGC cai no bash; subimos o JupyterLab manualmente (usado no `docker compose up`,
# modo interativo/inspecao). O caminho headless de correcao usa os scripts em bin/.
EXPOSE 8888
EXPOSE 4040
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=''", "--notebook-dir=/tf"]
