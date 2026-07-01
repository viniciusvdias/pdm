# Single-container image that runs the deterministic SSSP benchmark.
# Build context is the group root (finalproject/20261/pos); see docker-compose.yml.
FROM python:3.12-slim

WORKDIR /app

# System libs required by matplotlib/scipy wheels at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY misc/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/ /app/src/

# Defaults (overridable via docker-compose environment / env vars).
ENV GRAPH_DIR=/data \
    OUTPUT_DIR=/output \
    ORIGINS=8 \
    REPETITIONS=3 \
    WORKERS=1,2,4 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/app/src/run_experiments.py"]
