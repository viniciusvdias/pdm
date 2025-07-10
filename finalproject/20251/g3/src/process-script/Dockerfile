FROM python:3.10-slim

RUN apt-get update && apt-get install -y openjdk-17-jdk

RUN pip3 install pyspark matplotlib pandas

WORKDIR /app

COPY main.py .

RUN mkdir -p datalake/trusted datalake/refined datalake/refined_raw

CMD ["python3", "main.py"]