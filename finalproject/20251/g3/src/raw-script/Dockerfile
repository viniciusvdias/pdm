FROM python:3.9-slim

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY main.py .

RUN mkdir -p datalake/raw

CMD ["python", "main.py"]