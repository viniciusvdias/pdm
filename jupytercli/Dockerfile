FROM python:3.12-alpine
WORKDIR /app
#RUN apk add --update --no-cache bash build-base python3 python3-dev musl-dev linux-headers py3-pip procps openjdk11 && ln -sf python3 /usr/bin/python
RUN apk add --update --no-cache bash build-base linux-headers musl-dev procps openjdk11 && ln -sf python3 /usr/bin/python
ENV PYTHONUNBUFFERED=1
RUN python3 -m venv .venv
RUN source .venv/bin/activate && pip install pyspark==3.5.4 neo4j kafka-python jupyterlab networkx
COPY entrypoint.sh /bin/entrypoint.sh
RUN chmod +x /bin/entrypoint.sh
EXPOSE 8888
EXPOSE 4040
ENTRYPOINT ["/bin/entrypoint.sh"]
