FROM python:3.12-alpine
WORKDIR /app
RUN apk add --update --no-cache git bash build-base linux-headers musl-dev procps && ln -sf python3 /usr/bin/python
RUN git clone https://github.com/Aiven-Labs/python-fake-data-producer-for-apache-kafka.git
WORKDIR /app/python-fake-data-producer-for-apache-kafka
ENV PYTHONUNBUFFERED=1
RUN python3 -m venv .venv
RUN source .venv/bin/activate && pip install -r requirements.txt
COPY entrypoint.sh /bin/entrypoint.sh
RUN chmod +x /bin/entrypoint.sh
ENTRYPOINT ["/bin/entrypoint.sh"]
