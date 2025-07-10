# Use a imagem base do OpenJDK 17 com Python
FROM openjdk:17-jdk-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    python3 \
    python3-venv \
    curl \
    wget \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Criar symlink para python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Instalar UV da imagem oficial distroless (versão específica para builds reproduzíveis)
COPY --from=ghcr.io/astral-sh/uv:0.7.16 /uv /uvx /bin/

# Configurar variáveis de ambiente para otimização do UV
# UV_COMPILE_BYTECODE=1: Compila bytecode na instalação para melhor performance de startup
# UV_LINK_MODE=copy: Usa cópia em vez de hard links (necessário para cache mounts)
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Configurar JAVA_HOME
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH=$PATH:$JAVA_HOME/bin


ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" $SPARK_HOME \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"


RUN wget -q "https://repo1.maven.org/maven2/ch/cern/sparkmeasure/spark-measure_2.12/0.25/spark-measure_2.12-0.25.jar" \
    -O $SPARK_HOME/jars/spark-measure_2.12-0.25.jar

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências com cache mount otimizado (sem instalar o projeto ainda)
# Cache mount: acelera builds subsequentes reutilizando downloads do UV
# Bind mounts: evita copiar arquivos desnecessariamente para esta etapa
# --no-install-project: instala apenas dependências, não o projeto (para melhor cache)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-install-project

# Configurar variáveis de ambiente do Spark para usar o ambiente virtual
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python

# Copiar código fonte (em layer separada para não invalidar cache das dependências)
COPY . .

# Instalar o projeto no ambiente virtual já criado
# Cache mount: reutiliza cache do UV para melhor performance
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# Expor portas (Spark UI)
EXPOSE 4040 8080

CMD ["bash"] 