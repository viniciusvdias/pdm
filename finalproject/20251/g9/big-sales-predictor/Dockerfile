# Base com Java 17 já instalado
FROM openjdk:17-jdk-slim

# Instalar Python e pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Linka python3 como "python"
RUN ln -s /usr/bin/python3 /usr/bin/python

# Instala dependências Python
RUN pip install pyspark pandas

# Define JAVA_HOME (por segurança)
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copia os arquivos do projeto
COPY . /app
WORKDIR /app

# Comando principal (container fica "vivo")
CMD ["tail", "-f", "/dev/null"]
