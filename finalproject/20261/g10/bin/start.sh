#!/bin/bash
# bin/start.sh

# Raíz;
cd "$(dirname "$0")/.." || exit

TOPIC_NAME="wikimediaRecentchange"

echo "--- --------------- ---"
echo "Starting the project..."
echo "--- --------------- ---"

docker compose up -d

echo "Waiting for Kafka..."
while [ "$(docker inspect -f '{{.State.Health.Status}}' kafka)" != "healthy" ]; do
    sleep 1
done

# Iniciando tópicos do Kafka;
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ${TOPIC_NAME} \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Kafka ready, topic created successfully: '${TOPIC_NAME}'!"
