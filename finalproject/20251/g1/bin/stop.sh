#!/bin/bash

# Stop ENEM Spark Pipeline
# This script stops all running services and cleans up volumes

set -e

echo "Stopping Docker Compose services..."
cd misc
docker-compose down -v

echo "Cleaning up Docker images (optional)..."
read -p "Do you want to remove the Docker image? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker rmi enem-spark-job || echo "Image not found or already removed"
fi

echo "All services stopped successfully!"
cd ..
