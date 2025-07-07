FROM bitnami/spark:3.5

USER root

# Instala o Jupyter e dependências Python
RUN apt-get update && apt-get install -y python3-pip && \
    pip install notebook pyspark pandas matplotlib seaborn && \
    rm -rf /var/lib/apt/lists/*

# Diretório de trabalho padrão
WORKDIR /opt/jupyter

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--no-browser", "--allow-root", "--NotebookApp.token=spark123", "--NotebookApp.notebook_dir=/opt/jupyter"]
