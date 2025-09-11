#!/bin/bash

echo "------------News sentiment pipeline------------"

if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running."
    exit 1
fi

if [ ! -f .env ]; then
    echo ".env file not found."
    exit 1
fi


echo "Setting up Airflow.."
mkdir -p ./dags ./logs ./plugins ./config ./kibana
echo "AIRFLOW_UID=$(id -u)" > .env.airflow

echo "Starting Docker services..."
docker-compose -f docker-compose.yml up -d


echo "Zookeeper..."
while ! docker exec zookeeper bash -c 'echo ruok | nc localhost 2181' | grep -q imok; do
    sleep 2
done

echo "Kafka..."
sleep 10
while ! docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 2
done

echo "MongoDB..."
while ! docker exec mongodb mongosh --quiet --eval "db.adminCommand('ismaster')" > /dev/null 2>&1; do
    sleep 2
done

echo "PostgreSQL..."
while ! docker exec airflow-postgres pg_isready -U airflow > /dev/null 2>&1; do
    sleep 2
done

echo "Elasticsearch..."
timeout=60
while ! curl -s http://localhost:9200 > /dev/null 2>&1; do
    sleep 3
    timeout=$((timeout-3))
    if [ $timeout -le 0 ]; then
        break
    fi
done

echo "Kibana..."
timeout=60
while ! curl -s http://localhost:5601 > /dev/null 2>&1; do
    sleep 3
    timeout=$((timeout-3))
    if [ $timeout -le 0 ]; then
        break
    fi
done

echo "Airflow..."
timeout=120
while ! curl -s http://localhost:8081/health > /dev/null 2>&1; do
    sleep 5
    timeout=$((timeout-5))
    if [ $timeout -le 0 ]; then
        break
    fi
done

echo "Creating Kafka topics..."
bash scripts/create_topics.sh

echo "Setting up Elasticsearch index..."
curl -X PUT "localhost:9200/news-articles" -H 'Content-Type: application/json' -d'{
  "mappings": {
    "properties": {
      "title": {"type": "text", "analyzer": "english"},
      "description": {"type": "text", "analyzer": "english"},
      "content": {"type": "text", "analyzer": "english"},
      "url": {"type": "keyword"},
      "source": {
        "properties": {
          "api": {"type": "keyword"},
          "name": {"type": "keyword"},
          "url": {"type": "keyword"}
        }
      },
      "author": {"type": "keyword"},
      "published_at": {"type": "date"},
      "fetched_at": {"type": "date"},
      "processed_at": {"type": "date"},
      "sentiment": {
        "properties": {
          "overall": {"type": "keyword"},
          "confidence": {"type": "float"},
          "vader_compound": {"type": "float"},
          "textblob_polarity": {"type": "float"}
        }
      },
      "stats": {
        "properties": {
          "word_count": {"type": "integer"},
          "char_count": {"type": "integer"},
          "has_content": {"type": "boolean"}
        }
      },
      "timestamp": {"type": "date"}
    }
  }
}' > /dev/null 2>&1

echo ""
echo "ALL SERVICES ARE READY!!!"