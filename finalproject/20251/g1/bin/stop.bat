@echo off
REM Stop ENEM Spark Pipeline
REM This script stops all running services and cleans up volumes

echo Stopping Docker Compose services...
cd misc
docker-compose down -v

echo Cleaning up Docker images (optional)...
set /p cleanup="Do you want to remove the Docker image? (y/N): "
if /i "%cleanup%"=="y" (
    docker rmi enem-spark-job 2>nul || echo Image not found or already removed
)

echo ✅ All services stopped successfully!
cd ..
