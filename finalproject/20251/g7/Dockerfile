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
    git \
    libffi-dev \
    py3-numpy \
    py3-matplotlib \
    py3-pillow \
    py3-pip \
    py3-wheel \
    py3-setuptools \
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
    matplotlib \
    seaborn \
    streamlit \
    plotly \
    gdown

# Copia código e dados do projeto para dentro da imagem
COPY ./src /app/src
COPY ./datasample /app/datasample

# Scripts
COPY ./bin/entrypoint.sh ./bin/entrypoint.sh
COPY ./bin/exec-streamlit.sh ./bin/exec-streamlit.sh
RUN chmod +x ./bin/*.sh

# Portas do Jupyter, Streamlit e Spark Application Web UI
EXPOSE 8888
EXPOSE 8501
EXPOSE 4040

ENTRYPOINT ["./bin/entrypoint.sh"]
