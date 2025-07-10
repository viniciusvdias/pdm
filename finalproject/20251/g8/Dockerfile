# Use uma versão de Python estável e bem suportada pelas bibliotecas de dados
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
    pyspark==4.0.0 \
    jupyterlab \
    pandas \
    matplotlib \
    seaborn \
    streamlit \
    plotly \
    gdown

COPY ./src /app/src
COPY ./full_data /app/full_data

EXPOSE 8888 8501 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]