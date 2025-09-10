#!/bin/bash

echo "Creating Kafka topics..."

while ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "Waiting for Kafka..."
    sleep 2
done

echo "Kafka is ready. Creating topics..."

# news-raw topic
docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic news-raw \
    --config retention.ms=604800000 \
    --config compression.type=gzip \
    --if-not-exists

# news-processed topic  
docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic news-processed \
    --config retention.ms=1209600000 \
    --config compression.type=gzip \
    --if-not-exists

echo "Verifying topics creation..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo "Kafka topics created successfully!"