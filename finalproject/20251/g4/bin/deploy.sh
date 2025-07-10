#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="daskapp"
COMPOSE_FILE="src/docker/docker-compose.yml"

echo "Building custom dask-notebook image..."
docker build -t custom-dask-notebook:latest -f src/docker/Dockerfile .

cat > $COMPOSE_FILE << EOF
version: "3.9"

services:
  dask-scheduler:
    image: custom-dask-notebook:latest
    command: dask-scheduler
    ports:
      - "8786:8786"
      - "8787:8787"
    networks:
      - dask-net
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8787/status"]
      interval: 10s
      timeout: 3s
      retries: 5
  dask-worker:
    image: custom-dask-notebook:latest
    deploy:
      replicas: 6
    command: > 
      dask-worker tcp://dask-scheduler:8786
      --nthreads 4
      --memory-limit 12GB
    networks:
      - dask-net
    volumes:
      - ${PWD}/datasample:/home/jovyan/work/datasample

  jupyter:
    image: custom-dask-notebook:latest
    ports:
      - "8888:8888"
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - JUPYTER_TOKEN=token
    volumes:
      - ${PWD}/bin:/home/jovyan/work/bin
      - ${PWD}/datasample:/home/jovyan/work/datasample
      - ${PWD}/misc:/home/jovyan/work/misc
      - ${PWD}/src:/home/jovyan/work/src
    depends_on:
      - dask-scheduler
    networks:
      - dask-net

networks:
  dask-net:

volumes:
  notebook-data:
EOF

if ! docker info | grep -q 'Swarm: active'; then
  echo "Initializing Docker Swarm..."
  docker swarm init
else
  echo "Swarm já ativo."
fi

echo "Deploying stack '$STACK_NAME'..."
docker stack deploy -c $COMPOSE_FILE $STACK_NAME

echo "Deployment completo!"

echo "Acesse JupyterLab: http://localhost:8888"
