FROM python:3.13.5-alpine

WORKDIR /app

# Dependências
RUN apk update && apk add --no-cache \
    openjdk17 \
    build-base \
    linux-headers \
    musl-dev \
    curl \
    wget \
    bash \
    procps \
    && ln -sf python3 /usr/bin/python

# Variáveis de ambiente
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Ambiente virtual
RUN python3 -m venv .venv

# Dependências Python
RUN source .venv/bin/activate && pip install \
    pyspark==4.0.0 \
    jupyterlab \
    pandas \
    matplotlib

# Entrypoint
COPY bin/entrypoint.sh /bin/entrypoint.sh
RUN chmod +x /bin/entrypoint.sh

# Portas do Jupyter e Spark Application Web UI
EXPOSE 8888
EXPOSE 4040

ENTRYPOINT ["/bin/entrypoint.sh"]
