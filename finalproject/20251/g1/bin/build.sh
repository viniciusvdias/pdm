#!/bin/bash

# Build only the Docker image
# This script builds the Docker image without running the pipeline

set -e

echo "Building ENEM Spark Job Docker image..."
docker build -t enem-spark-job -f misc/Dockerfile .

echo "Docker image built successfully!"
echo "Image name: enem-spark-job:latest"
echo ""
echo "To run the pipeline, use: ./bin/run.sh"
