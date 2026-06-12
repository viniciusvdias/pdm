# Imagem leve para producer, consumer, baseline e harness de experimentos.
FROM python:3.11-slim

WORKDIR /app
COPY misc/app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
ENV PYTHONPATH=/app/src

# Comando default sobrescrito por cada serviço no docker-compose.
CMD ["python", "-c", "print('app image ready')"]
