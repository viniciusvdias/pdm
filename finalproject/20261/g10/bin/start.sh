#!/bin/bash
# bin/start.sh

# Raíz;
cd "$(dirname "$0")/.." || exit

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo ".env not found! Exiting..."
    exit 1
fi

echo "--- --------------- ---"
echo "Starting the project..."
echo "--- --------------- ---"

docker compose up -d --build

echo "Waiting for Kafka..."
while [ "$(docker inspect -f '{{.State.Health.Status}}' kafka)" != "healthy" ]; do
    sleep 1
done

# Iniciando tópicos do Kafka;
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic "$KAFKA_TOPIC" \
  --bootstrap-server localhost:9092 \
  --partitions "$KAFKA_PARTITIONS" \
  --replication-factor "$KAFKA_REPLICATION_FACTOR" \
  --if-not-exists

echo "Kafka ready, topic created successfully: '${KAFKA_TOPIC}'!"


echo "Waiting for Spark consumer..."
while [ "$(docker inspect -f '{{.State.Health.Status}}' spark_consumer)" != "healthy" ]; do
    sleep 1
done

echo "Spark consumer ready!"

echo "Logs:"
echo "Producer: docker logs -f wikimedia_producer"
echo "Spark Consumer: docker logs -f spark_consumer"
