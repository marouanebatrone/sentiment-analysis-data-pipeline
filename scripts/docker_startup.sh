#!/bin/bash

echo "=========================================="
echo "News sentiment analsis data pipeline"
echo "=========================================="

if ! docker info > /dev/null 2>&1; then
    echo " Docker is not running. Please start Docker first."
    exit 1
fi

if [ ! -f .env ]; then
    echo " .env file not found. Please create it with your API keys."
    echo " Copy .env.example to .env and add your API keys"
    exit 1
fi

echo "Starting Docker containers..."

docker-compose up -d

echo "Waiting for all the services to be ready..."

echo "Waiting for Zookeeper..."
while ! docker exec zookeeper bash -c 'echo ruok | nc localhost 2181' | grep -q imok; do
    sleep 2
done
echo "Zookeeper is ready"

echo "Waiting for Kafka..."
sleep 10
while ! docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 2
done
echo "Kafka is ready"

echo "Waiting for MongoDB..."
while ! docker exec mongodb mongosh --quiet --eval "db.adminCommand('ismaster')" > /dev/null 2>&1; do
    sleep 2
done
echo "MongoDB is ready"

echo "Creating Kafka topics..."
bash scripts/create_topics.sh
echo ""
echo "All services are ready!"
echo ""